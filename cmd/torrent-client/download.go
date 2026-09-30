package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"torrent-client/internal/content"
	"torrent-client/internal/engine"
	"torrent-client/internal/magnet"
	"torrent-client/internal/metadata"
	"torrent-client/internal/metainfo"
	"torrent-client/internal/ratelimit"
	"torrent-client/internal/seed"
	"torrent-client/internal/state"
	"torrent-client/internal/storage"
	"torrent-client/internal/tracker"
	"torrent-client/internal/ui"
)

// seedSource feeds the upload path from the download engine and the content
// store: the engine says which pieces are verified and servable, the store
// hands out their bytes. The engine is swapped on a tracker retry, so the
// pointer is guarded.
type seedSource struct {
	store *storage.Storage

	mu  sync.Mutex
	eng *engine.Engine
}

func (s *seedSource) setEngine(eng *engine.Engine) {
	s.mu.Lock()
	s.eng = eng
	s.mu.Unlock()
}

func (s *seedSource) Have(index int) bool {
	s.mu.Lock()
	eng := s.eng
	s.mu.Unlock()
	return eng != nil && eng.Have(index)
}

func (s *seedSource) HaveBitfield() []byte {
	s.mu.Lock()
	eng := s.eng
	s.mu.Unlock()
	if eng == nil {
		return nil
	}
	return eng.HaveBitfield()
}

func (s *seedSource) ReadBlock(index int, begin, length uint32) ([]byte, error) {
	return s.store.ReadBlock(index, begin, length)
}

// execute runs a validated invocation: load the metainfo, pick a tracker,
// download, report the exit code. Anything that goes wrong here is a runtime
// failure, so it's exitFatal.
func execute(cfg config, stdout, stderr io.Writer) int {
	// The live display owns stderr from here on. Events go through it so they
	// scroll beneath the sticky progress line rather than being overwritten by
	// it; on a stream that can't take cursor control it falls back to the plain
	// appended lines the tests read.
	dash := ui.New(stderr)
	defer dash.Close()

	fail := func(err error) int {
		dash.Eventf("torrent-client: %v", err)
		return exitFatal
	}
	logf := func(format string, args ...any) {
		dash.Eventf("torrent-client: "+format, args...)
	}

	// Ctrl-C cancels cleanly: pieces already written stay on disk and the resume
	// sidecar carries them to the next run.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	peerID, err := newPeerID()
	if err != nil {
		return fail(fmt.Errorf("generate peer id: %w", err))
	}

	// A magnet carries only an info-hash, so the metadata is fetched from the
	// swarm first. Past this point both sources are just a torrent, and nothing
	// cares which one it came from.
	var meta *metainfo.MetaInfo
	if strings.HasPrefix(cfg.source, "magnet:") {
		meta, err = resolveMagnet(ctx, cfg.source, peerID, uint16(cfg.port), logf)
	} else {
		meta, err = metainfo.Load(cfg.source)
	}
	if err != nil {
		return fail(err)
	}
	trackers := meta.Trackers()
	if len(trackers) == 0 {
		return fail(fmt.Errorf("the torrent has no tracker to announce to"))
	}

	// Where the content lands, plus the store that speaks that layout. Both come
	// from internal/content, so the download and seed paths can't disagree about
	// which file a piece lives in.
	output, err := content.OutputFor(meta, cfg.output)
	if err != nil {
		return fail(err)
	}
	store, err := content.Open(meta, output)
	if err != nil {
		return fail(err)
	}
	defer store.Close()

	// The resume sidecar is keyed to this info-hash and this output path, so a
	// stray one from another torrent or output is never adopted. It sits beside
	// the output as "<output>.resume".
	resume := state.Open(output, meta.InfoHash, meta.Info.PieceLength, meta.TotalLength(), meta.PieceCount())

	// The redraw loop runs for the whole download; on a non-terminal stream it
	// returns at once and logf stays a plain line writer.
	go dash.Run(ctx)

	logf("downloading %q (%d pieces, %d bytes) to %s",
		meta.Info.Name, meta.PieceCount(), meta.TotalLength(), output)
	if cfg.maxDownRate > 0 {
		logf("download capped at %s", ui.HumanRate(float64(cfg.maxDownRate)))
	}

	// With -seed the client also uploads. The store and the listener come up
	// before the first announce, so the port we advertise is the one we serve on,
	// and the listener is live while we fetch — a piece becomes servable the
	// moment it verifies.
	var (
		srv       *seed.Server
		source    *seedSource
		upLimiter *ratelimit.Limiter
		uploaded  func() int64
	)
	if cfg.seed {
		// The upload direction gets its own bucket: capping the download must not
		// cap the pieces we serve, and vice versa.
		upLimiter = ratelimit.New(cfg.maxUpRate)

		source = &seedSource{store: store}
		// Bind every interface: peers reach us on whatever address the tracker
		// hands out, not on a loopback-only socket.
		srv, err = seed.New(seed.Config{
			Meta:    meta,
			Source:  source,
			PeerID:  peerID,
			Listen:  net.JoinHostPort("", strconv.Itoa(cfg.port)),
			Log:     logf,
			Limiter: upLimiter,
		})
		if err != nil {
			return fail(err)
		}
		defer srv.Close()
		uploaded = srv.Uploaded

		go func() {
			if err := srv.Serve(ctx); err != nil {
				logf("seed listener: %v", err)
			}
		}()
		logf("seeding on %s", srv.Addr())
		if cfg.maxUpRate > 0 {
			logf("upload capped at %s", ui.HumanRate(float64(cfg.maxUpRate)))
		}
	}

	// Try each announce URL in turn: one dead tracker isn't a reason to give up
	// while another knows the swarm.
	var lastErr error
	for _, announceURL := range trackers {
		// The URL scheme picks the wire protocol (http/https or udp); the engine
		// only ever sees the tracker.Tracker interface.
		tr, err := tracker.New(announceURL)
		if err != nil {
			logf("tracker %s is unusable: %v", announceURL, err)
			lastErr = err
			continue
		}
		eng := engine.New(engine.Config{
			Meta:        meta,
			PeerID:      peerID,
			Port:        uint16(cfg.port),
			Output:      output,
			Store:       store,
			Tracker:     tr,
			Log:         logf,
			Uploaded:    uploaded,
			Seed:        cfg.seed,
			Resume:      resume,
			MaxDownRate: cfg.maxDownRate,
		})
		if source != nil {
			source.setEngine(eng)
		}
		// Point the sticky line at this engine's counters while it runs; a retry
		// with the next tracker swaps the source. The engine reports the download
		// cap, but the upload cap lives in the seed server, so it's filled in here
		// just so the display can show limiting is on.
		upRate := int64(0)
		if cfg.seed {
			upRate = cfg.maxUpRate
		}
		dash.Track(func() engine.Stats {
			s := eng.Stats()
			s.MaxUpRate = upRate
			return s
		})
		runErr := eng.Run(ctx)
		// The engine has stopped announcing, so release the tracker's socket before
		// the next URL.
		if closer, ok := tr.(io.Closer); ok {
			_ = closer.Close()
		}
		if runErr != nil {
			if ctx.Err() != nil {
				// Ctrl-C mid-download is a clean stop, not a failure. The state flushed
				// above is enough to resume next time.
				dash.Eventf("torrent-client: interrupted; resume state saved for %s", output)
				return exitOK
			}
			logf("tracker %s failed: %v", announceURL, runErr)
			lastErr = runErr
			continue
		}
		lastErr = nil
		break
	}
	// No engine runs any more, so retire the progress line before the final
	// messages — otherwise a stale frame sits above them.
	dash.Track(nil)
	if lastErr != nil {
		return fail(fmt.Errorf("download failed: %w", lastErr))
	}

	// With -seed, Run returns only when the context is cancelled: until then the
	// client stayed in the swarm uploading.
	if cfg.seed {
		dash.Eventf("torrent-client: seeding stopped: %s", output)
		return exitOK
	}
	dash.Eventf("torrent-client: download complete: %s", output)
	return exitOK
}

// magnetTimeout bounds resolving a magnet: finding peers and pulling metadata
// is a negotiation, not the download, so it mustn't hang as long as a real
// transfer legitimately could.
const magnetTimeout = 90 * time.Second

// resolveMagnet turns a magnet into metainfo. The link carries only an
// info-hash, so we announce to one of its trackers, pull the info dictionary
// over ut_metadata, and check it against the hash we asked for. That check is
// the whole basis of trust: the hash is the only reason to believe bytes a
// stranger sent, and it stops a hostile peer serving a different torrent.
func resolveMagnet(ctx context.Context, uri string, peerID [20]byte, port uint16, logf func(string, ...any)) (*metainfo.MetaInfo, error) {
	m, err := magnet.Parse(uri)
	if err != nil {
		return nil, err
	}
	logf("resolving magnet %x", m.InfoHash)

	ctx, cancel := context.WithTimeout(ctx, magnetTimeout)
	defer cancel()

	var lastErr error
	for _, announceURL := range m.Trackers {
		tr, err := tracker.New(announceURL)
		if err != nil {
			logf("tracker %s is unusable: %v", announceURL, err)
			lastErr = err
			continue
		}
		resp, err := tracker.AnnounceWithRetry(ctx, tr, tracker.AnnounceRequest{
			InfoHash: m.InfoHash,
			PeerID:   peerID,
			Port:     port,
			// We don't know the size yet, but a non-zero Left is what stops a tracker
			// counting us as a seeder. The engine reports the real numbers once it
			// takes the swarm over.
			Left:    1,
			Event:   tracker.EventStarted,
			NumWant: 50,
		}, tracker.DefaultRetry)
		if closer, ok := tr.(io.Closer); ok {
			_ = closer.Close()
		}
		if err != nil {
			logf("tracker %s had nothing for this info-hash: %v", announceURL, err)
			lastErr = err
			continue
		}

		addrs := make([]string, 0, len(resp.Peers))
		for _, p := range resp.Peers {
			addrs = append(addrs, p.Addr())
		}
		logf("%d peers offered metadata for %x", len(addrs), m.InfoHash)

		info, err := metadata.Fetch(ctx, m.InfoHash, peerID, addrs)
		if err != nil {
			lastErr = err
			continue
		}

		meta, err := metainfo.ParseInfoBytes(info, m.Trackers)
		if err != nil {
			return nil, err
		}
		if meta.InfoHash != m.InfoHash {
			return nil, fmt.Errorf("metainfo: the fetched info-hash %x is not the magnet's %x", meta.InfoHash, m.InfoHash)
		}
		logf("metadata: %q, %d pieces, %d bytes", meta.Info.Name, meta.PieceCount(), meta.TotalLength())
		return meta, nil
	}

	if lastErr == nil {
		lastErr = errors.New("the magnet names no usable tracker")
	}
	return nil, fmt.Errorf("magnet: %w", lastErr)
}

// newPeerID builds an Azureus-style id: a client tag plus random bytes.
func newPeerID() ([20]byte, error) {
	var id [20]byte
	copy(id[:], "-TC0001-")
	if _, err := rand.Read(id[8:]); err != nil {
		return id, err
	}
	return id, nil
}
