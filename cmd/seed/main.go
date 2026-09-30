// Command seed is the demo seeder for the local swarm: it holds a fixture and
// serves it over the real peer protocol so a download can be verified offline.
//
// It is a harness counterpart to cmd/torrent-client, not a general-purpose
// server: it unchokes anyone who asks and answers every request it can.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"time"

	"torrent-client/internal/metainfo"
	"torrent-client/internal/storage"
	"torrent-client/internal/tracker"
	"torrent-client/internal/wire"
)

const (
	handshakeTimeout = 10 * time.Second
	// maxRequestLength is the classic protocol ceiling for a single request.
	maxRequestLength = 128 * 1024
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

	// Bind first so the port we announce is the port we actually serve on.
	ln, err := net.Listen("tcp", net.JoinHostPort(listen, strconv.Itoa(port)))
	if err != nil {
		return err
	}
	defer ln.Close()
	boundPort := ln.Addr().(*net.TCPAddr).Port

	peerID, err := newPeerID()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	tr := tracker.NewHTTP(trackerURL)
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
		meta.Info.Name, meta.PieceCount(), meta.TotalLength(), ln.Addr())
	fmt.Printf("seed: announced to %s (interval %s)\n", trackerURL, resp.Interval)

	interval := resp.Interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	go reannounce(ctx, tr, meta, peerID, uint16(boundPort), interval)

	fmt.Println("seed: ready")
	if err := serve(ctx, ln, meta, store, peerID, serveDelay); err != nil {
		return err
	}

	// Best-effort goodbye so the tracker drops us immediately.
	_, _ = tr.Announce(context.Background(), tracker.AnnounceRequest{
		InfoHash: meta.InfoHash,
		PeerID:   peerID,
		Port:     uint16(boundPort),
		Left:     0,
		Event:    tracker.EventStopped,
	})
	return nil
}

func serve(ctx context.Context, ln net.Listener, meta *metainfo.MetaInfo, store *storage.Storage, peerID [20]byte, serveDelay time.Duration) error {
	all := wire.BitfieldComplete(meta.PieceCount())
	for {
		conn, err := ln.Accept()
		if err != nil {
			// A cancelled context closes the listener, which is a clean exit.
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go serveConn(conn, meta, store, peerID, all, serveDelay)
	}
}

func serveConn(conn net.Conn, meta *metainfo.MetaInfo, store *storage.Storage, peerID [20]byte, all []byte, serveDelay time.Duration) {
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return
	}
	if _, err := conn.Write(wire.NewHandshake(meta.InfoHash, peerID).Encode()); err != nil {
		return
	}
	if _, err := wire.ReadHandshake(conn, meta.InfoHash); err != nil {
		return // not our swarm: drop silently
	}
	// Clear the deadline: a seeding connection sits idle between requests.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return
	}

	r := bufio.NewReader(conn)
	if err := wire.Write(conn, wire.Message{ID: wire.IDBitfield, Bitfield: all}); err != nil {
		return
	}

	// This fixture unchokes anyone who asks: with a single peer there is no
	// upload slot to compete for.
	interested := false
	for {
		m, err := wire.Decode(r)
		if err != nil {
			return
		}
		switch m.ID {
		case wire.IDInterested:
			if !interested {
				if err := wire.Write(conn, wire.Message{ID: wire.IDUnchoke}); err != nil {
					return
				}
				interested = true
			}
		case wire.IDNotInterested:
			interested = false
		case wire.IDRequest:
			if serveDelay > 0 {
				time.Sleep(serveDelay)
			}
			if err := serveRequest(conn, meta, store, m); err != nil {
				return
			}
		}
		// Choke/unchoke/have/bitfield/cancel/keep-alive from the client do not
		// change what we serve.
	}
}

func serveRequest(conn net.Conn, meta *metainfo.MetaInfo, store *storage.Storage, m wire.Message) error {
	index := int(m.Index)
	if index < 0 || index >= meta.PieceCount() {
		return fmt.Errorf("request for piece %d is out of range for %d pieces", index, meta.PieceCount())
	}
	if m.Length == 0 || m.Length > maxRequestLength {
		return fmt.Errorf("request length %d is outside 1..%d", m.Length, maxRequestLength)
	}
	if int64(m.Begin)+int64(m.Length) > meta.PieceSize(index) {
		return fmt.Errorf("request %d+%d overruns piece %d of %d bytes", m.Begin, m.Length, index, meta.PieceSize(index))
	}

	data, err := store.ReadBlock(index, m.Begin, m.Length)
	if err != nil {
		return err
	}
	if err := wire.Write(conn, wire.Message{ID: wire.IDPiece, Index: m.Index, Begin: m.Begin, Block: data}); err != nil {
		return err
	}
	// Logged on purpose: the swarm tests read these lines to prove which
	// seeder carried which block, and when.
	fmt.Printf("seed: served piece=%d begin=%d length=%d\n", m.Index, m.Begin, len(data))
	return nil
}

func reannounce(ctx context.Context, tr *tracker.HTTPTracker, meta *metainfo.MetaInfo, peerID [20]byte, port uint16, interval time.Duration) {
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
