package tracker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"
)

// Peer is one swarm member: an address we can dial over the peer wire
// protocol.
type Peer struct {
	IP   net.IP
	Port uint16
}

// Addr renders the peer as host:port.
func (p Peer) Addr() string {
	return net.JoinHostPort(p.IP.String(), strconv.Itoa(int(p.Port)))
}

// Event is the announce lifecycle event. The empty event is a periodic
// re-announce.
type Event string

const (
	EventStarted   Event = "started"
	EventCompleted Event = "completed"
	EventStopped   Event = "stopped"
)

// AnnounceRequest carries everything a tracker needs about us. Uploaded and
// Downloaded are the session counters; Left is what still remains.
type AnnounceRequest struct {
	InfoHash   [20]byte
	PeerID     [20]byte
	Port       uint16
	Uploaded   int64
	Downloaded int64
	Left       int64
	Event      Event
	NumWant    int
}

// AnnounceResponse is the decoded reply: the peers to try plus the tracker's
// view of the swarm.
type AnnounceResponse struct {
	Interval    time.Duration
	MinInterval time.Duration
	Complete    int
	Incomplete  int
	Peers       []Peer
}

// Tracker is a swarm source. HTTP is the first transport; a UDP transport
// (BEP 15) plugs in behind the same interface without the engine changing.
type Tracker interface {
	Announce(ctx context.Context, req AnnounceRequest) (AnnounceResponse, error)
}

// Error classifies an announce failure. A transient failure is worth retrying;
// a fatal one (a tracker that rejects our info-hash) never will be.
type Error struct {
	Transient bool
	Err       error
}

func (e *Error) Error() string { return e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

// IsFatal reports whether err is a tracker error that retrying cannot fix.
// Unknown error types are treated as transient, because a download should not
// die over an unclassified hiccup.
func IsFatal(err error) bool {
	var te *Error
	return errors.As(err, &te) && !te.Transient
}

// RetryPolicy bounds the backoff applied to transient announce failures.
type RetryPolicy struct {
	Attempts  int
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

// DefaultRetry is the policy the engine uses for a single announce.
var DefaultRetry = RetryPolicy{Attempts: 3, BaseDelay: 500 * time.Millisecond, MaxDelay: 5 * time.Second}

// AnnounceWithRetry calls t until it answers, the policy is exhausted, or the
// context is cancelled. Fatal tracker errors stop immediately; everything else
// backs off exponentially. The engine re-announces on its own schedule, so
// exhausting the attempts here never kills a download in progress.
func AnnounceWithRetry(ctx context.Context, t Tracker, req AnnounceRequest, p RetryPolicy) (AnnounceResponse, error) {
	if p.Attempts < 1 {
		p.Attempts = 1
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = time.Second
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = 30 * time.Second
	}

	delay := p.BaseDelay
	var lastErr error
	for attempt := 0; attempt < p.Attempts; attempt++ {
		resp, err := t.Announce(ctx, req)
		if err == nil {
			return resp, nil
		}
		if IsFatal(err) {
			return AnnounceResponse{}, err
		}
		lastErr = err
		if attempt == p.Attempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return AnnounceResponse{}, ctx.Err()
		case <-time.After(delay):
		}
		if delay *= 2; delay > p.MaxDelay {
			delay = p.MaxDelay
		}
	}
	return AnnounceResponse{}, fmt.Errorf("announce failed after %d attempts: %w", p.Attempts, lastErr)
}
