package ratelimit

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

// throughput runs total bytes through the limiter in chunk-sized reservations
// and returns the wall-clock throughput in bytes per second.
func throughput(t *testing.T, l *Limiter, total, chunk int) float64 {
	t.Helper()
	ctx := context.Background()
	start := time.Now()
	for sent := 0; sent < total; sent += chunk {
		if err := l.Wait(ctx, chunk); err != nil {
			t.Fatalf("Wait: %v", err)
		}
	}
	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		t.Fatalf("transfer took no time at all")
	}
	return float64(total) / elapsed
}

// The cap has to hold over a real transfer, and in both directions: a limiter
// that ignores the rate is as wrong as one that stalls forever.
func TestCapHoldsOverAMeasuredTransfer(t *testing.T) {
	const rate = int64(512 * 1024)
	const total = 1 << 20 // 1 MiB at 512 KiB/s is two seconds
	l := New(rate)

	got := throughput(t, l, total, 16*1024)
	if got > 1.5*float64(rate) {
		t.Fatalf("measured %.0f B/s, want no more than 1.5x the %d B/s cap", got, rate)
	}
	if got < 0.5*float64(rate) {
		t.Fatalf("measured %.0f B/s, want at least 0.5x the %d B/s cap (stalled?)", got, rate)
	}
}

// An unset rate must leave throughput unconstrained: the whole transfer has to
// finish in a fraction of the time the same bytes take under a cap.
func TestUnlimitedRateNeverBlocks(t *testing.T) {
	l := New(0)
	if l.Rate() != 0 {
		t.Fatalf("New(0) reports a %d B/s rate, want unlimited", l.Rate())
	}
	start := time.Now()
	if err := l.Wait(context.Background(), 64<<20); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("an unlimited limiter blocked for %s", d)
	}
	burstStart := time.Now()
	for sent := 0; sent < 8<<20; sent += 16 * 1024 {
		if err := l.Wait(context.Background(), 16*1024); err != nil {
			t.Fatalf("Wait: %v", err)
		}
	}
	if d := time.Since(burstStart); d > time.Second {
		t.Fatalf("8 MiB of reservations took %s unlimited, so something is pacing them", d)
	}
}

// An idle limiter may spend its burst allowance at once and no more: the
// schedule lags the clock by one burst window, not by however long it sat
// unused.
func TestIdleCreditStopsAtTheBurstAllowance(t *testing.T) {
	// At 8 MiB/s the burst allowance (capped at 1 MiB) is 125 ms, so a short
	// sleep already dwarfs it.
	const rate = int64(8 << 20)
	l := New(rate)
	burst := burstDuration(rate)
	if want := 125 * time.Millisecond; burst != want {
		t.Fatalf("burst window at %d B/s is %s, want %s", rate, burst, want)
	}

	time.Sleep(5 * burst)

	if err := l.Wait(context.Background(), 1); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	l.mu.Lock()
	lag := time.Since(l.head)
	l.mu.Unlock()

	if lag > 2*burst {
		t.Fatalf("schedule lagged the clock by %s after an idle spell, want at most the %s burst allowance", lag, burst)
	}
	if lag < burst/2 {
		t.Fatalf("schedule lagged the clock by only %s, want the full %s burst allowance to have accrued", lag, burst)
	}
}

// A cancelled download must not sit in the bucket: Wait returns as soon as the
// context is done, and the waiting goroutine goes away.
func TestWaitReturnsOnCancelWithoutLeakingAGoroutine(t *testing.T) {
	l := New(1024) // 1 KiB/s: 1 MiB fills the schedule for a quarter of an hour
	// The first reservation starts the schedule and is admitted at once, so it
	// is spent here to push the next one genuinely far into the future.
	if err := l.Wait(context.Background(), 1<<20); err != nil {
		t.Fatalf("prime: %v", err)
	}
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Wait(ctx, 16*1024) }()

	// Let the waiter reach the sleep before cancelling: we want the blocked
	// path exercised, not a lucky race.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after the context was cancelled")
	}

	// The waiting goroutine has to be gone, and unrelated ones come and go with
	// the test runtime, so settle briefly instead of trusting a single count.
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("goroutines went from %d to %d: the cancelled waiter leaked", before, n)
	}
}

// Lowering a cap mid-transfer cannot un-send what is already scheduled: a
// reservation handed out at the old rate keeps its slot, while reservations
// made after the change are paced at the new one.
func TestLoweringTheRateOnlyPacesLaterReservations(t *testing.T) {
	ctx := context.Background()

	t.Run("in-flight reservations keep their slots", func(t *testing.T) {
		l := New(1 << 20) // 1 MiB/s
		// Prime the schedule: the first reservation is free, so it is spent
		// here, leaving 1 MiB (one second) queued ahead of the test.
		if err := l.Wait(ctx, 1<<20); err != nil {
			t.Fatal(err)
		}

		done := make(chan time.Duration, 1)
		go func() {
			start := time.Now()
			if err := l.Wait(ctx, 512*1024); err != nil {
				done <- -1
				return
			}
			done <- time.Since(start)
		}()
		time.Sleep(100 * time.Millisecond) // the reservation has landed
		l.SetRate(64 * 1024)               // 512 KiB at the new rate is 8 s

		select {
		case d := <-done:
			if d < 0 {
				t.Fatal("Wait failed")
			}
			if d > 4*time.Second {
				t.Fatalf("an already-scheduled reservation was re-timed to %s by the lower cap", d)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the in-flight reservation never returned")
		}
	})

	t.Run("later reservations use the new rate", func(t *testing.T) {
		l := New(1 << 20)
		l.SetRate(64 * 1024) // 16 KiB takes 250 ms

		start := time.Now()
		for i := 0; i < 4; i++ {
			if err := l.Wait(ctx, 16*1024); err != nil {
				t.Fatal(err)
			}
		}
		elapsed := time.Since(start)
		if elapsed < 400*time.Millisecond {
			t.Fatalf("four reservations took %s at the new 64 KiB/s cap, want at least 400ms", elapsed)
		}
		if elapsed > 5*time.Second {
			t.Fatalf("four reservations took %s, want about 750ms", elapsed)
		}
	})
}

// Fairness is arrival order: a reservation made later cannot start before one
// made earlier, so a peer with a deep pipeline cannot jump the queue.
func TestLaterReservationCannotOvertakeAnEarlierOne(t *testing.T) {
	const rate = int64(512 * 1024)
	ctx := context.Background()
	l := New(rate)
	// Spend the free first slot so both measured reservations actually queue.
	if err := l.Wait(ctx, 512*1024); err != nil {
		t.Fatal(err)
	}

	first := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		if err := l.Wait(ctx, 512*1024); err != nil {
			first <- -1
			return
		}
		first <- time.Since(start)
	}()

	time.Sleep(50 * time.Millisecond) // the first reservation lands first
	start := time.Now()
	if err := l.Wait(ctx, 16*1024); err != nil {
		t.Fatal(err)
	}
	second := time.Since(start)

	a := <-first
	if a < 0 {
		t.Fatal("the first reservation failed")
	}
	if a < 700*time.Millisecond {
		t.Fatalf("the first reservation waited only %s, want about a second", a)
	}
	if second <= a {
		t.Fatalf("the later reservation waited %s but the earlier one waited %s: it overtook the queue", second, a)
	}
}
