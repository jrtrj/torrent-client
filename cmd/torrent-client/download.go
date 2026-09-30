package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"torrent-client/internal/engine"
	"torrent-client/internal/metainfo"
	"torrent-client/internal/ratelimit"
	"torrent-client/internal/seed"
	"torrent-client/internal/state"
	"torrent-client/internal/storage"
	"torrent-client/internal/tracker"
	"torrent-client/internal/ui"
)

// seedSource adapts the download engine and the content store to the upload
// path: the engine says which pieces are verified and servable, the store
// hands out their bytes. The engine is replaced when the client retries with
// another tracker, so the pointer is guarded.
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
// download the content, and report the exit code. Everything that can go
// wrong here is a runtime failure, so it maps to exitFatal.
func execute(cfg config, stdout, stderr io.Writer) int {
	// The live display owns stderr from here on. Events are reported through
	// it so they scroll beneath the sticky progress line instead of being
	// overwritten by it; on a stream that cannot take cursor control it
	// degrades to the plain appended lines the tests read.
	dash := ui.New(stderr)
	defer dash.Close()

	fail := func(err error) int {
		dash.Eventf("torrent-client: %v", err)
		return exitFatal
	}

	// Magnet links reach the same pipeline later (metadata first, then this
	// path); until then they are a clear fatal error rather than a confusing
	// "no such file".
	if strings.HasPrefix(cfg.source, "magnet:") {
		return fail(fmt.Errorf("magnet links are not implemented yet"))
	}

	meta, err := metainfo.Load(cfg.source)
	if err != nil {
		return fail(err)
	}
	if meta.Multifile() {
		return fail(fmt.Errorf("multi-file torrents are not implemented yet"))
	}
	trackers := meta.Trackers()
	if len(trackers) == 0 {
		return fail(fmt.Errorf("the torrent has no tracker to announce to"))
	}

	peerID, err := newPeerID()
	if err != nil {
		return fail(fmt.Errorf("generate peer id: %w", err))
	}
	output := resolveOutput(cfg.output, meta.Info.Name)

	// The resume sidecar is keyed to this torrent's info-hash and this output
	// path, so a leftover sidecar from another torrent or another output is
	// never adopted. It lives beside the output as "<output>.resume".
	resume := state.Open(output, meta.InfoHash, meta.Info.PieceLength, meta.TotalLength(), meta.PieceCount())

	// Ctrl-C cancels cleanly; the pieces already written stay on disk and the
	// resume sidecar keeps them for the next run.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// The redraw loop runs for the whole download; on a non-terminal stream it
	// returns immediately and the logf below stays a plain line writer.
	go dash.Run(ctx)

	logf := func(format string, args ...any) {
		dash.Eventf("torrent-client: "+format, args...)
	}
	logf("downloading %q (%d pieces, %d bytes) to %s",
		meta.Info.Name, meta.PieceCount(), meta.TotalLength(), output)
	if cfg.maxDownRate > 0 {
		logf("download capped at %s", ui.HumanRate(float64(cfg.maxDownRate)))
	}

	// With -seed the client also uploads. The content store and the inbound
	// listener come up before the first announce, so the port we advertise is
	// the port we actually serve on and the listener is live while we fetch,
	// which is what lets a piece become servable the moment it verifies.
	var (
		store     *storage.Storage
		srv       *seed.Server
		source    *seedSource
		upLimiter *ratelimit.Limiter
		uploaded  func() int64
	)
	if cfg.seed {
		store, err = storage.Open(output, meta.Info.PieceLength, meta.TotalLength())
		if err != nil {
			return fail(fmt.Errorf("open %s for seeding: %w", output, err))
		}
		// The store outlives the engine: the upload path reads the same file
		// the download writes, so this is the one owner.
		defer store.Close()

		// The upload direction gets its own bucket: throttling the download
		// must not throttle the pieces we serve, and the other way round.
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

	// Try each announce URL in turn: a tracker that is down is not a reason to
	// give up while another one knows the swarm.
	var lastErr error
	for _, announceURL := range trackers {
		// The URL scheme picks the wire protocol (http/https or udp); the
		// engine only ever sees the tracker.Tracker interface.
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
		// Point the sticky line at this engine's counters for as long as it
		// runs; a retry with the next tracker replaces the source. The engine
		// reports the download cap; the upload cap lives in the seed server
		// and is filled in here, so the display can show that limiting is on.
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
		// The engine has stopped announcing, so release the tracker's socket
		// before moving on to the next URL.
		if closer, ok := tr.(io.Closer); ok {
			_ = closer.Close()
		}
		if runErr != nil {
			if ctx.Err() != nil {
				// Ctrl-C during the download: a clean stop, not a failure.
				// The state flushed above is enough to resume next time.
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
	// No engine is running any more, so retire the progress line before the
	// final messages; otherwise a stale frame would sit above them.
	dash.Track(nil)
	if lastErr != nil {
		return fail(fmt.Errorf("download failed: %w", lastErr))
	}

	// With -seed, Run returns only when the context is cancelled: the client
	// stayed in the swarm, uploading, until then.
	if cfg.seed {
		dash.Eventf("torrent-client: seeding stopped: %s", output)
		return exitOK
	}
	dash.Eventf("torrent-client: download complete: %s", output)
	return exitOK
}

// resolveOutput decides where a single-file torrent's content goes. A path
// that names an existing directory, or that ends in a separator, holds the
// file under the torrent's name; anything else is the file itself.
func resolveOutput(output, name string) string {
	if strings.HasSuffix(output, string(os.PathSeparator)) {
		return filepath.Join(output, name)
	}
	if info, err := os.Stat(output); err == nil && info.IsDir() {
		return filepath.Join(output, name)
	}
	return output
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
