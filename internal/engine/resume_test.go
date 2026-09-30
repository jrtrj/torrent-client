package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"torrent-client/internal/metainfo"
	"torrent-client/internal/state"
	"torrent-client/internal/storage"
	"torrent-client/internal/tracker"
	"torrent-client/internal/wire"
)

// seedOutput writes the chosen pieces of data straight into the output file, as
// a previous run would have left them, and nothing else. Pieces not listed stay
// zero, which is exactly the state a killed download leaves behind.
func seedOutput(t *testing.T, out string, meta *metainfo.MetaInfo, data []byte, pieces ...int) {
	t.Helper()
	store, err := storage.Open(out, meta.Info.PieceLength, meta.TotalLength())
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer store.Close()
	for _, i := range pieces {
		off := int64(i) * meta.Info.PieceLength
		if err := store.WritePiece(i, data[off:off+meta.PieceSize(i)]); err != nil {
			t.Fatalf("write piece %d: %v", i, err)
		}
	}
}

// haveBitfield builds a verified-piece bitfield with the given pieces set.
func haveBitfield(pieces int, set ...int) []byte {
	b := make([]byte, (pieces+7)/8)
	for _, i := range set {
		wire.BitfieldSet(b, i)
	}
	return b
}

// TestResumeAdoptsVerifiedPiecesAndAdvertisesThem is the scheduler half of the
// resume proof: a piece the sidecar claims and the file still proves is adopted
// rather than fetched, is advertised to peers in the opening bitfield, and the
// sidecar is removed once the download completes.
func TestResumeAdoptsVerifiedPiecesAndAdvertisesThem(t *testing.T) {
	const pieceLength = 64 * 1024
	data := testPayload(t, 3*pieceLength)
	meta := testTorrent(t, data, pieceLength)

	out := filepath.Join(t.TempDir(), "out.bin")
	seedOutput(t, out, meta, data, 0)

	resume := state.Open(out, meta.InfoHash, meta.Info.PieceLength, meta.TotalLength(), meta.PieceCount())
	if err := resume.Save(haveBitfield(meta.PieceCount(), 0)); err != nil {
		t.Fatalf("save sidecar: %v", err)
	}

	seeder := newFakeSeeder(t, "seeder", meta, data, seederConfig{advertise: []int{0, 1, 2}})
	run := startEngine(t, meta, []tracker.Peer{seeder.peer()}, func(c *Config) {
		c.Output = out
		c.Resume = resume
	})
	run.wait(t, 20*time.Second)

	readOutput(t, out, data)
	if stats := run.eng.Stats(); stats.PiecesDone != 3 {
		t.Fatalf("verified %d of 3 pieces", stats.PiecesDone)
	}
	if seeder.servedCount() == 0 {
		t.Fatal("the seeder served nothing, so the resume path was not exercised")
	}
	for _, b := range seeder.servedBlocks() {
		if b.piece == 0 {
			t.Fatalf("the seeder re-served block %d+%d of already-held piece 0", b.begin, b.length)
		}
	}
	if !seeder.clientHas(0) {
		t.Fatalf("the client did not advertise held piece 0; opening bitfield %v", seeder.clientBitsSeen())
	}
	if _, err := resume.Load(); !errors.Is(err, state.ErrNoState) {
		t.Fatalf("sidecar survived a completed download: %v", err)
	}
}

// TestResumeDropsTamperedPieceAndRefetches is the verify-then-trust half: the
// sidecar claims piece 0, the content file no longer matches, so the claim must
// be rejected and the piece fetched again rather than trusted.
func TestResumeDropsTamperedPieceAndRefetches(t *testing.T) {
	const pieceLength = 64 * 1024
	data := testPayload(t, 3*pieceLength)
	meta := testTorrent(t, data, pieceLength)

	out := filepath.Join(t.TempDir(), "out.bin")
	seedOutput(t, out, meta, data, 0)

	// Corrupt piece 0 on disk while the sidecar still claims it verified.
	f, err := os.OpenFile(out, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	if _, err := f.WriteAt([]byte{^data[0]}, 0); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	f.Close()

	resume := state.Open(out, meta.InfoHash, meta.Info.PieceLength, meta.TotalLength(), meta.PieceCount())
	if err := resume.Save(haveBitfield(meta.PieceCount(), 0)); err != nil {
		t.Fatalf("save sidecar: %v", err)
	}

	seeder := newFakeSeeder(t, "seeder", meta, data, seederConfig{advertise: []int{0, 1, 2}})
	run := startEngine(t, meta, []tracker.Peer{seeder.peer()}, func(c *Config) {
		c.Output = out
		c.Resume = resume
	})
	run.wait(t, 20*time.Second)

	readOutput(t, out, data)
	refetched := false
	for _, b := range seeder.servedBlocks() {
		if b.piece == 0 {
			refetched = true
		}
	}
	if !refetched {
		t.Fatal("the tampered piece was never re-fetched, so re-verification did not run")
	}
}

// TestResumeIgnoresForeignSidecar checks the keying rule at the engine: a
// sidecar for another torrent at the same output path is not adopted, so the
// whole download is fetched as if it were fresh.
func TestResumeIgnoresForeignSidecar(t *testing.T) {
	const pieceLength = 64 * 1024
	data := testPayload(t, 2*pieceLength)
	meta := testTorrent(t, data, pieceLength)

	out := filepath.Join(t.TempDir(), "out.bin")
	seedOutput(t, out, meta, data, 0, 1)

	var otherHash [20]byte
	otherHash[0] = 0xEE
	resume := state.Open(out, otherHash, meta.Info.PieceLength, meta.TotalLength(), meta.PieceCount())
	// The bytes on disk are this torrent's, but the sidecar names a different
	// torrent, so it must be ignored rather than adopted.
	foreign := state.Open(out, meta.InfoHash, meta.Info.PieceLength, meta.TotalLength(), meta.PieceCount())
	if err := foreign.Save(haveBitfield(meta.PieceCount(), 0, 1)); err != nil {
		t.Fatalf("save sidecar: %v", err)
	}

	seeder := newFakeSeeder(t, "seeder", meta, data, seederConfig{advertise: []int{0, 1}})
	run := startEngine(t, meta, []tracker.Peer{seeder.peer()}, func(c *Config) {
		c.Output = out
		c.Resume = resume
	})
	run.wait(t, 20*time.Second)

	readOutput(t, out, data)
	if seeder.servedCount() != 8 {
		t.Fatalf("seeder served %d blocks, want all 8: a foreign sidecar was adopted", seeder.servedCount())
	}
}

// flakyTracker answers its first announces and then blocks until the context
// is cancelled, standing in for a tracker that dies mid-download.
type flakyTracker struct {
	mu         sync.Mutex
	calls      int
	blockAfter int
}

func (f *flakyTracker) Announce(ctx context.Context, _ tracker.AnnounceRequest) (tracker.AnnounceResponse, error) {
	f.mu.Lock()
	f.calls++
	call := f.calls
	f.mu.Unlock()
	if call > f.blockAfter {
		<-ctx.Done()
		return tracker.AnnounceResponse{}, ctx.Err()
	}
	return tracker.AnnounceResponse{Interval: time.Hour}, nil
}

func (f *flakyTracker) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// TestShutdownAnnounceIsBounded proves the goodbye announce cannot hang the
// process: with a tracker that stops answering, Run still returns promptly
// after the context is cancelled.
func TestShutdownAnnounceIsBounded(t *testing.T) {
	const pieceLength = 64 * 1024
	data := testPayload(t, pieceLength)
	meta := testTorrent(t, data, pieceLength)
	stub := &flakyTracker{blockAfter: 1}

	eng := New(Config{
		Meta:    meta,
		Output:  filepath.Join(t.TempDir(), "out.bin"),
		Tracker: stub,
		Log:     t.Logf,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := make(chan error, 1)
	go func() { errs <- eng.Run(ctx) }()

	// Let the initial announce land, then cancel while the engine is waiting
	// for peers that never arrive.
	// Generous budgets: these assert liveness (returns at all, advertised at
	// all), and a loaded CI box can delay goroutines by seconds. A tight
	// window turns a scheduling hiccup into a false failure.
	deadline := time.Now().Add(30 * time.Second)
	for stub.callCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errs:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(stoppedAnnounceTimeout + 30*time.Second):
		t.Fatalf("Run did not return within %s of cancel: the stopped announce is not bounded", stoppedAnnounceTimeout+30*time.Second)
	}
	if got := stub.callCount(); got < 2 {
		t.Fatalf("tracker saw %d announces, want at least the initial one plus the stopped one", got)
	}
}
