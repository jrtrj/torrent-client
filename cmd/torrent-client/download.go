package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"torrent-client/internal/engine"
	"torrent-client/internal/metainfo"
	"torrent-client/internal/tracker"
)

// execute runs a validated invocation: load the metainfo, pick a tracker,
// download the content, and report the exit code. Everything that can go
// wrong here is a runtime failure, so it maps to exitFatal.
func execute(cfg config, stdout, stderr io.Writer) int {
	fail := func(err error) int {
		fmt.Fprintf(stderr, "torrent-client: %v\n", err)
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

	// Ctrl-C cancels cleanly; the pieces already written stay on disk.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	logf := func(format string, args ...any) {
		fmt.Fprintf(stderr, "torrent-client: "+format+"\n", args...)
	}
	logf("downloading %q (%d pieces, %d bytes) to %s",
		meta.Info.Name, meta.PieceCount(), meta.TotalLength(), output)

	// Try each announce URL in turn: a tracker that is down is not a reason to
	// give up while another one knows the swarm.
	var lastErr error
	for _, announceURL := range trackers {
		eng := engine.New(engine.Config{
			Meta:    meta,
			PeerID:  peerID,
			Port:    uint16(cfg.port),
			Output:  output,
			Tracker: tracker.NewHTTP(announceURL),
			Log:     logf,
		})
		if err := eng.Run(ctx); err != nil {
			if ctx.Err() != nil {
				return fail(ctx.Err())
			}
			logf("tracker %s failed: %v", announceURL, err)
			lastErr = err
			continue
		}
		lastErr = nil
		break
	}
	if lastErr != nil {
		return fail(fmt.Errorf("download failed: %w", lastErr))
	}

	fmt.Fprintf(stderr, "torrent-client: download complete: %s\n", output)
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
