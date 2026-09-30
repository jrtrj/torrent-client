// Package ratelimit is the client's per-direction bandwidth limiter.
//
// A token bucket holds capacity C and refills at R bytes per second, one byte
// at a time. This limiter runs the same policy in the equivalent "virtual
// schedule" form (generic cell rate algorithm): instead of a token count a
// clock tops up, it stores the wall-clock time at which the bucket will next
// have room, and every caller is handed its own future slot to sleep until.
// Both forms admit exactly the same byte stream; the schedule form is used
// because two properties fall out of it for free.
//
// Fairness is arrival order. Slots are handed out under one mutex, so a caller
// cannot overtake one that got there first, and each connection has at most one
// reservation outstanding (both pumps are serial per connection), so no peer
// can queue ahead of another more than once. A deep request pipeline therefore
// cannot starve a shallow one.
//
// Cancellation cannot corrupt the bucket. A caller that gives up — a cancelled
// download, a dead connection — leaves its slot unspent, putting the schedule
// slightly ahead of the bytes actually sent, so the limiter can only ever
// under-send, never over-send. Wait is context-aware, selecting on the caller's
// context and on the timer for its own slot, and returns ctx.Err() the moment
// the context is done. That timer reaching the reserved instant is the "token
// is ready" signal, so there is no wakeup channel and no servo goroutine to
// leak.
//
// Capacity (burst): the schedule may lag real time by up to one second of the
// configured rate, capped at 1 MiB, so an idle connection can spend that much
// in one go instead of being paced from a standing start. One second admits a
// whole in-flight request window (eight 16 KiB blocks by default), so the
// limiter paces a transfer instead of stalling per block; the 1 MiB ceiling
// bounds how far a restart after a long idle spell can jump past the cap.
//
// A reservation is admitted whole at the start of its slot, so the limiter
// paces in units of one request and the schedule is always one reservation
// ahead of the bytes sent. Callers pass bounded amounts — one 16 KiB block
// down, at most one 128 KiB request up — so that excess is a fixed one request,
// not a hole in the cap: over k requests the measured rate is R*k/(k-1).
package ratelimit

import (
	"context"
	"sync"
	"time"
)

const (
	// maxBurstBytes caps the burst allowance: a limiter that has been idle for
	// ages must not be able to jump far past its cap.
	maxBurstBytes = 1 << 20
	// burstWindow is the lag the schedule may accumulate while idle: one
	// second of the configured rate.
	burstWindow = time.Second
)

// Limiter is one direction's token bucket. A zero rate means unlimited, in
// which case Wait never blocks. All methods are safe for concurrent use.
type Limiter struct {
	mu sync.Mutex
	// rate is bytes per second; 0 means unlimited.
	rate int64
	// head is when the bucket will next be free, and the schedule every
	// reservation is appended to. It starts at construction, so the first
	// reservation is served at once; it falls behind the clock only while
	// nobody is waiting, and that lag is the burst credit.
	head time.Time
}

// New returns a limiter for rate bytes per second, or an unlimited one when
// rate is zero or negative.
func New(rate int64) *Limiter {
	return &Limiter{rate: max(rate, 0), head: time.Now()}
}

// Rate is the configured cap in bytes per second; zero means unlimited.
func (l *Limiter) Rate() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rate
}

// SetRate changes the cap for every reservation made after the call. Slots
// already handed out keep the deadline they were given — a request already in
// the schedule cannot be un-sent — so lowering a cap mid-transfer only
// stretches things from the next reservation on.
func (l *Limiter) SetRate(rate int64) {
	l.mu.Lock()
	l.rate = max(rate, 0)
	l.mu.Unlock()
}

// Wait blocks until n bytes may be sent without exceeding the cap, or until
// ctx is done, whichever comes first. An unlimited limiter returns at once.
//
// The reservation is made under the lock and the sleep happens outside it, so
// callers are ordered without holding each other's mutex while they wait.
func (l *Limiter) Wait(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	l.mu.Lock()
	rate := l.rate
	if rate <= 0 {
		l.mu.Unlock()
		return nil
	}
	now := time.Now()
	// An idle bucket accrues credit up to the burst allowance; drifting
	// further behind than that would let a cap be spent all at once.
	if earliest := now.Add(-burstDuration(rate)); l.head.Before(earliest) {
		l.head = earliest
	}
	start := l.head
	l.head = start.Add(scale(n, rate))
	l.mu.Unlock()

	// start can be in the past when the bucket had credit: nothing to wait for.
	if delay := time.Until(start); delay > 0 {
		t := time.NewTimer(delay)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// burstDuration is how long the schedule may lag real time: one second of the
// configured rate, never more than maxBurstBytes.
func burstDuration(rate int64) time.Duration {
	d := scale(int(maxBurstBytes), rate)
	if d > burstWindow {
		return burstWindow
	}
	return d
}

// scale is the time n bytes take at rate bytes per second.
func scale(n int, rate int64) time.Duration {
	return time.Duration(float64(n) / float64(rate) * float64(time.Second))
}
