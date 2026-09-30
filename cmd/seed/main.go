// Command seed is the demo seeder for the local swarm: it holds a fixture and
// serves it over the real peer protocol so a download can be verified offline.
//
// It is a harness counterpart to cmd/torrent-client, not a general-purpose
// server. The serving itself lives in internal/seed, the same package the real
// client uses, so there is one upload implementation rather than two.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"time"

	"torrent-client/internal/metainfo"
	"torrent-client/internal/seed"
	"torrent-client/internal/storage"
	"torrent-client/internal/tracker"
	"torrent-client/internal/wire"
)

const usageText = `seed — serve a .torrent's data over the peer wire protocol.

Usage:
  seed [flags] <torrent-file> <data-path>

Flags:
  -tracker string
        announce URL (default: the torrent's own announce)
  -port int
        TCP port to listen on; 0 picks a free one (default 0)
  -listen string
        interface to bind (default 127.0.0.1)
  -serve-delay duration
        sleep before answering each block request (e.g. 15ms); used by the
        swarm tests to widen the serving window of each seeder
`

func main() {
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usageText) }
	trackerURL := fs.String("tracker", "", "announce URL")
	port := fs.Int("port", 0, "TCP port to listen on")
	listen := fs.String("listen", "127.0.0.1", "interface to bind")
	serveDelay := fs.Duration("serve-delay", 0, "sleep before answering each block request")
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	if fs.NArg() != 2 {
		fmt.Fprint(os.Stderr, usageText)
		os.Exit(2)
	}

	if err := run(fs.Arg(0), fs.Arg(1), *trackerURL, *listen, *port, *serveDelay); err != nil {
		fmt.Fprintf(os.Stderr, "seed: %v\n", err)
		os.Exit(1)
	}
}

// completeSource serves every piece of a torrent that is fully on disk.
type completeSource struct {
	store  *storage.Storage
	pieces int
	all    []byte
}

func newCompleteSource(store *storage.Storage, pieces int) completeSource {
	return completeSource{store: store, pieces: pieces, all: wire.BitfieldComplete(pieces)}
}

func (s completeSource) Have(index int) bool { return index >= 0 && index < s.pieces }

func (s completeSource) HaveBitfield() []byte { return s.all }

func (s completeSource) ReadBlock(index int, begin, length uint32) ([]byte, error) {
	return s.store.ReadBlock(index, begin, length)
}

func run(torrentPath, dataPath, trackerURL, listen string, port int, serveDelay time.Duration) error {
	meta, err := metainfo.Load(torrentPath)
	if err != nil {
		return err
	}
	if meta.Multifile() {
		return errors.New("multi-file torrents are not supported yet")
	}
	if trackerURL == "" {
		urls := meta.Trackers()
		if len(urls) == 0 {
			return errors.New("the torrent has no tracker; pass -tracker")
		}
		trackerURL = urls[0]
	}

	store, err := storage.Open(dataPath, meta.Info.PieceLength, meta.TotalLength())
	if err != nil {
		return err
	}
	defer store.Close()

	peerID, err := newPeerID()
	if err != nil {
		return err
	}

	// Bind first so the port we announce is the port we actually serve on.
	srv, err := seed.New(seed.Config{
		Meta:       meta,
		Source:     newCompleteSource(store, meta.PieceCount()),
		PeerID:     peerID,
		Listen:     net.JoinHostPort(listen, strconv.Itoa(port)),
		ServeDelay: serveDelay,
		Log:        func(format string, args ...any) { fmt.Printf("seed: "+format+"\n", args...) },
	})
	if err != nil {
		return err
	}
	defer srv.Close()
	boundPort := int(srv.Port())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// The torrent's announce URL picks the transport (http/https or udp).
	tr, err := tracker.New(trackerURL)
	if err != nil {
		return err
	}
	if closer, ok := tr.(io.Closer); ok {
		defer closer.Close()
	}
	// We hold every piece, so announce with left=0: the tracker counts us
	// complete and hands our address to leechers straight away.
	resp, err := tr.Announce(ctx, tracker.AnnounceRequest{
		InfoHash: meta.InfoHash,
		PeerID:   peerID,
		Port:     uint16(boundPort),
		Left:     0,
		Event:    tracker.EventStarted,
		NumWant:  50,
	})
	if err != nil {
		return fmt.Errorf("announce: %w", err)
	}

	fmt.Printf("seed: serving %q (%d pieces, %d bytes) on %s\n",
		meta.Info.Name, meta.PieceCount(), meta.TotalLength(), srv.Addr())
	fmt.Printf("seed: announced to %s (interval %s)\n", trackerURL, resp.Interval)

	interval := resp.Interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	go reannounce(ctx, tr, meta, peerID, srv, uint16(boundPort), interval)

	fmt.Println("seed: ready")
	if err := srv.Serve(ctx); err != nil {
		return err
	}

	// Best-effort goodbye so the tracker drops us immediately. The deadline
	// bounds a lost datagram: a UDP announce retries on its own schedule and
	// must not hold up the exit.
	goodbye, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = tr.Announce(goodbye, tracker.AnnounceRequest{
		InfoHash: meta.InfoHash,
		PeerID:   peerID,
		Port:     uint16(boundPort),
		Left:     0,
		Event:    tracker.EventStopped,
	})
	return nil
}

func reannounce(ctx context.Context, tr tracker.Tracker, meta *metainfo.MetaInfo, peerID [20]byte, srv *seed.Server, port uint16, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := tr.Announce(ctx, tracker.AnnounceRequest{
				InfoHash: meta.InfoHash,
				PeerID:   peerID,
				Port:     port,
				Uploaded: srv.Uploaded(),
				Left:     0,
				NumWant:  50,
			}); err != nil {
				fmt.Fprintf(os.Stderr, "seed: re-announce failed: %v\n", err)
			}
		}
	}
}

// newPeerID builds an Azureus-style id: a client tag plus random bytes. The
// bytes are arbitrary binary and the tracker encodes them as such.
func newPeerID() ([20]byte, error) {
	var id [20]byte
	copy(id[:], "-TC0001-")
	if _, err := rand.Read(id[8:]); err != nil {
		return id, err
	}
	return id, nil
}
