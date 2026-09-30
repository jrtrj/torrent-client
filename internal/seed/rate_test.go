package seed

import (
	"bytes"
	"context"
	"testing"
	"time"

	"torrent-client/internal/metainfo"
	"torrent-client/internal/ratelimit"
	"torrent-client/internal/wire"
)

// newLimitedServer starts a server with an upload limiter (nil for none), so a
// test can run the same scripted peer against both paths.
func newLimitedServer(t *testing.T, meta *metainfo.MetaInfo, src Source, limiter *ratelimit.Limiter) *Server {
	t.Helper()
	srv, err := New(Config{
		Meta:    meta,
		Source:  src,
		PeerID:  testPeerID(),
		Listen:  "127.0.0.1:0",
		Log:     t.Logf,
		Limiter: limiter,
	})
	if err != nil {
		t.Fatalf("seed.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		srv.Close()
	})
	go func() { _ = srv.Serve(ctx) }()
	return srv
}

// fetchAll requests every block of the torrent in order and returns the bytes
// the peer received.
func fetchAll(t *testing.T, p *peer, meta *metainfo.MetaInfo) ([]byte, time.Duration) {
	t.Helper()
	got := make([]byte, meta.TotalLength())
	start := time.Now()
	for i := 0; i < meta.PieceCount(); i++ {
		size := meta.PieceSize(i)
		off := int64(i) * meta.Info.PieceLength
		for begin := int64(0); begin < size; begin += wire.BlockSize {
			length := min(int64(wire.BlockSize), size-begin)
			p.write(wire.Message{ID: wire.IDRequest, Index: uint32(i), Begin: uint32(begin), Length: uint32(length)})
			m := p.expect(wire.IDPiece)
			if int64(len(m.Block)) != length {
				t.Fatalf("block %d+%d is %d bytes, want %d", i, begin, len(m.Block), length)
			}
			copy(got[off+begin:], m.Block)
		}
	}
	return got, time.Since(start)
}

// The seeding side has to be capped too, not just the download side: a server
// with an upload cap must pace its replies and still deliver every byte.
func TestUploadCapHoldsOverAServedDownload(t *testing.T) {
	const rate = int64(256 * 1024)
	data := series(512 * 1024) // two seconds at the cap
	meta := testTorrent(t, data, testPieceLength)
	store := testStore(t, meta, data)
	src := newTestSource(store, meta)
	src.holdAll(meta.PieceCount())
	srv := newLimitedServer(t, meta, src, ratelimit.New(rate))

	p := dialSeed(t, srv, meta)
	defer p.close()
	p.expect(wire.IDBitfield)
	p.write(wire.Message{ID: wire.IDInterested})
	p.expect(wire.IDUnchoke)

	got, elapsed := fetchAll(t, p, meta)

	if !bytes.Equal(got, data) {
		t.Fatalf("served data differs from the source")
	}
	if up := srv.Uploaded(); up != int64(len(data)) {
		t.Fatalf("uploaded %d bytes, want %d: the cap dropped bytes", up, len(data))
	}
	throughput := float64(len(data)) / elapsed.Seconds()
	if throughput > 1.5*float64(rate) {
		t.Fatalf("served at %.0f B/s over %s, want no more than 1.5x the %d B/s cap", throughput, elapsed, rate)
	}
	if throughput < 0.5*float64(rate) {
		t.Fatalf("served at %.0f B/s over %s, want at least 0.5x the %d B/s cap", throughput, elapsed, rate)
	}
}

// With no limiter the same payload must cross loopback unpaced, which is the
// contrast that shows the previous test measures the limiter and not the wire.
func TestUploadWithoutALimiterIsNotPaced(t *testing.T) {
	data := series(512 * 1024)
	meta := testTorrent(t, data, testPieceLength)
	store := testStore(t, meta, data)
	src := newTestSource(store, meta)
	src.holdAll(meta.PieceCount())
	srv := newLimitedServer(t, meta, src, nil)

	p := dialSeed(t, srv, meta)
	defer p.close()
	p.expect(wire.IDBitfield)
	p.write(wire.Message{ID: wire.IDInterested})
	p.expect(wire.IDUnchoke)

	got, elapsed := fetchAll(t, p, meta)
	if !bytes.Equal(got, data) {
		t.Fatalf("served data differs from the source")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("uncapped server took %s for 512 KiB, so something paced it", elapsed)
	}
}

// A peer that stops reading must not pin a serving goroutine inside the
// limiter: stopping the server has to release a wait that is half a minute away
// from being satisfied, on both the context and the Close path.
func TestStoppingAServerReleasesAThrottledPeer(t *testing.T) {
	const rate = int64(1024)
	data := series(2 * testPieceLength)
	meta := testTorrent(t, data, testPieceLength)
	store := testStore(t, meta, data)
	src := newTestSource(store, meta)
	src.holdAll(meta.PieceCount())

	for _, stop := range []struct {
		name    string
		release func(srv *Server, cancel context.CancelFunc)
	}{
		{"context cancel", func(_ *Server, cancel context.CancelFunc) { cancel() }},
		{"Close", func(srv *Server, _ context.CancelFunc) { _ = srv.Close() }},
	} {
		t.Run(stop.name, func(t *testing.T) {
			srv, err := New(Config{
				Meta:    meta,
				Source:  src,
				PeerID:  testPeerID(),
				Listen:  "127.0.0.1:0",
				Log:     t.Logf,
				Limiter: ratelimit.New(rate),
			})
			if err != nil {
				t.Fatalf("seed.New: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			served := make(chan struct{})
			go func() {
				_ = srv.Serve(ctx)
				close(served)
			}()
			t.Cleanup(func() { _ = srv.Close() })

			p := dialSeed(t, srv, meta)
			defer p.close()
			p.expect(wire.IDBitfield)
			p.write(wire.Message{ID: wire.IDInterested})
			p.expect(wire.IDUnchoke)
			// The first block is admitted at once (it starts the schedule);
			// the second is 32 seconds behind it at 1 KiB/s, so the serving
			// goroutine is asleep in the limiter when the server stops.
			p.write(wire.Message{ID: wire.IDRequest, Index: 0, Begin: 0, Length: 32 * 1024})
			p.expect(wire.IDPiece)
			p.write(wire.Message{ID: wire.IDRequest, Index: 0, Begin: 32768, Length: 32 * 1024})
			if err := p.conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if _, err := wire.Decode(p.r); err == nil {
				t.Fatal("the second block was answered, so the upload cap was never applied")
			}

			began := time.Now()
			stop.release(srv, cancel)
			select {
			case <-served:
			case <-time.After(5 * time.Second):
				t.Fatal("Serve did not return")
			}
			if err := srv.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if d := time.Since(began); d > 5*time.Second {
				t.Fatalf("shutdown took %s: a serving goroutine is stuck in the limiter", d)
			}
		})
	}
}
