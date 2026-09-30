package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"torrent-client/internal/tracker"
)

// These tests drive a real download over loopback with a capped rate, so what
// they prove is the enforcement itself: the transfer is paced near the cap, it
// still finishes, and the request pipeline stays full while it is paced.
//
// The bands are wide on purpose (0.5x to 1.5x): the measurement is wall-clock
// time on a shared box, and the property is "paced", not "paced to three decimals".

func allPieces(n int) []int {
	held := make([]int, n)
	for i := range held {
		held[i] = i
	}
	return held
}

// samplePipeline polls a running download; call the returned function to stop
// it and read the deepest outstanding request window and busiest peer count.
func samplePipeline(eng *Engine) func() (outstanding, active int) {
	var (
		mu          sync.Mutex
		outstanding int
		active      int
	)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				st := eng.Stats()
				mu.Lock()
				if out := int(st.BlocksRequested - st.BlocksReceived); out > outstanding {
					outstanding = out
				}
				if st.PeersActive > active {
					active = st.PeersActive
				}
				mu.Unlock()
			}
		}
	}()
	return func() (int, int) {
		close(stop)
		<-done
		mu.Lock()
		defer mu.Unlock()
		return outstanding, active
	}
}

// The cap must hold on a real transfer, in both directions of the band, while
// requests stay in flight: a limiter that stalls would fail the completion and
// the throughput floor, and one that is ignored would fail the ceiling.
func TestDownloadCapHoldsWithRequestsInFlight(t *testing.T) {
	const pieceLength = 256 * 1024
	const rate = int64(512 * 1024)
	data := testPayload(t, 2<<20) // four seconds at the cap
	meta := testTorrent(t, data, pieceLength)

	peer := newFakeSeeder(t, "peer", meta, data, seederConfig{advertise: allPieces(meta.PieceCount())})
	run := startEngine(t, meta, []tracker.Peer{peer.peer()}, func(c *Config) {
		c.MaxDownRate = rate
	})

	takeSample := samplePipeline(run.eng)
	start := time.Now()
	run.wait(t, 60*time.Second)
	elapsed := time.Since(start)
	peakOutstanding, peakActive := takeSample()

	readOutput(t, run.out, data)
	st := run.eng.Stats()
	if st.PiecesDone != meta.PieceCount() {
		t.Fatalf("verified %d of %d pieces", st.PiecesDone, meta.PieceCount())
	}
	if st.MaxDownRate != rate {
		t.Fatalf("stats report a %d B/s cap, want %d", st.MaxDownRate, rate)
	}

	got := float64(len(data)) / elapsed.Seconds()
	if got > 1.5*float64(rate) {
		t.Fatalf("measured %.0f B/s over %s, want no more than 1.5x the %d B/s cap", got, elapsed, rate)
	}
	if got < 0.5*float64(rate) {
		t.Fatalf("measured %.0f B/s over %s, want at least 0.5x the %d B/s cap", got, elapsed, rate)
	}

	// Paced, not dried up: the transfer was slower than the wire and yet had
	// blocks outstanding for most of it.
	if peakOutstanding < 2 {
		t.Fatalf("deepest outstanding window was %d, want the pipeline to stay full while pacing", peakOutstanding)
	}
	if peakActive < 1 {
		t.Fatalf("no peer was ever active while the cap paced the download")
	}
	if st.BlocksStalled != 0 {
		t.Fatalf("%d requests expired while pacing: the pipeline did not absorb the wait", st.BlocksStalled)
	}
}

// An unset cap must not pace anything: the same bytes that take seconds under a
// cap have to cross loopback in a fraction of that.
func TestUnlimitedDownloadIsNotPaced(t *testing.T) {
	const pieceLength = 256 * 1024
	data := testPayload(t, 2<<20) // four seconds under a 512 KiB/s cap
	meta := testTorrent(t, data, pieceLength)

	peer := newFakeSeeder(t, "peer", meta, data, seederConfig{advertise: allPieces(meta.PieceCount())})
	run := startEngine(t, meta, []tracker.Peer{peer.peer()}, nil)

	start := time.Now()
	run.wait(t, 60*time.Second)
	elapsed := time.Since(start)

	readOutput(t, run.out, data)
	if elapsed > 3*time.Second {
		t.Fatalf("uncapped download of 2 MiB took %s, so something paced it", elapsed)
	}
	if st := run.eng.Stats(); st.MaxDownRate != 0 {
		t.Fatalf("uncapped engine reports a %d B/s cap", st.MaxDownRate)
	}
}

// Cancelling a throttled download must not leave a pump asleep in the limiter:
// Run has to return promptly even though the next token is hours away.
func TestCancellingWhileThrottledReturnsPromptly(t *testing.T) {
	const pieceLength = 64 * 1024
	data := testPayload(t, 8<<20) // over two hours at 1 KiB/s
	meta := testTorrent(t, data, pieceLength)

	peer := newFakeSeeder(t, "peer", meta, data, seederConfig{advertise: allPieces(meta.PieceCount())})
	run := startEngine(t, meta, []tracker.Peer{peer.peer()}, func(c *Config) {
		c.MaxDownRate = 1024
	})

	// Long enough for the first block to arrive and the pump to go to sleep on
	// the next token.
	time.Sleep(300 * time.Millisecond)

	began := time.Now()
	run.cancel()
	select {
	case err := <-run.err:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after cancel: a pump is stuck in the limiter")
	}
	if d := time.Since(began); d > 5*time.Second {
		t.Fatalf("Run took %s to return after cancel", d)
	}
}

// A cap lowered mid-transfer cannot un-send what is already in the pipeline.
// The bounded per-peer window has to absorb those requests rather than the
// download stalling or losing them.
func TestLoweringTheCapMidDownloadStillCompletes(t *testing.T) {
	const pieceLength = 64 * 1024
	data := testPayload(t, 1<<20)
	meta := testTorrent(t, data, pieceLength)

	peer := newFakeSeeder(t, "peer", meta, data, seederConfig{advertise: allPieces(meta.PieceCount())})
	run := startEngine(t, meta, []tracker.Peer{peer.peer()}, func(c *Config) {
		c.MaxDownRate = 512 * 1024
	})

	// Let the pipeline fill, then drop the cap by half.
	time.Sleep(300 * time.Millisecond)
	run.eng.limiter.SetRate(256 * 1024)

	start := time.Now()
	run.wait(t, 60*time.Second)
	elapsed := time.Since(start)

	readOutput(t, run.out, data)
	st := run.eng.Stats()
	if st.PiecesDone != meta.PieceCount() {
		t.Fatalf("verified %d of %d pieces", st.PiecesDone, meta.PieceCount())
	}
	if st.BlocksStalled != 0 || st.BlocksDuplicate != 0 {
		t.Fatalf("lowering the cap lost work: %d stalled, %d duplicate", st.BlocksStalled, st.BlocksDuplicate)
	}
	if elapsed < time.Second {
		t.Fatalf("the rest of the transfer took %s, so the lower cap was never applied", elapsed)
	}
}
