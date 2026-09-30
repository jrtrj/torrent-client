package engine

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"torrent-client/internal/metainfo"
	"torrent-client/internal/storage"
	"torrent-client/internal/tracker"
	"torrent-client/internal/wire"
)

const (
	// defaultPipelineDepth is how many 16 KiB block requests may be in flight
	// on one connection. BEP 3 leaves the number to the client; a window of 8
	// keeps a peer busy across a WAN round trip without letting one slow peer
	// hold a large share of the request window.
	defaultPipelineDepth = 8
	// maxPipelineDepth caps a caller-supplied depth so the in-flight window
	// stays bounded whatever the caller asks for.
	maxPipelineDepth = 16

	dialTimeout      = 10 * time.Second
	handshakeTimeout = 10 * time.Second
	// idleTimeout bounds the wait for any message from a peer. It doubles as
	// the per-write deadline, so one wedged peer cannot pin a pump forever.
	idleTimeout = 30 * time.Second
	// defaultStallTimeout is the wall-clock grace a block request gets before
	// it is re-issued to another peer.
	defaultStallTimeout = 20 * time.Second
	// noProgressTimeout fails a download that cannot move forward, so a swarm
	// with no useful peers surfaces as an error instead of hanging.
	noProgressTimeout = 2 * time.Minute
	// swarmRetryDelay is the shortest gap between announces while no peer is
	// connected, so a peer that appears later is picked up quickly.
	swarmRetryDelay = time.Second
	// defaultAnnounceInterval is used when a tracker does not say how often to
	// re-announce.
	defaultAnnounceInterval = 30 * time.Second
	minAnnounceInterval     = time.Second
	maxAnnounceInterval     = 10 * time.Minute

	// maxDialers bounds simultaneous outbound dials.
	maxDialers = 8
	// eventBuffer and dialBuffer size the scheduler's inbound queues. They are
	// large enough that connection pumps never block on a busy scheduler.
	eventBuffer = 512
	dialBuffer  = 256
)

// Config is everything one download needs. The tracker is injected so the
// engine never constructs transports itself.
type Config struct {
	Meta    *metainfo.MetaInfo
	PeerID  [20]byte
	Port    uint16
	Output  string
	Tracker tracker.Tracker

	// Log receives one line per milestone. A nil Log discards them. Calls are
	// serialised, so a Log that is not goroutine-safe is still usable.
	Log func(format string, args ...any)

	// PipelineDepth is the per-connection in-flight block window. Zero means
	// defaultPipelineDepth; values above maxPipelineDepth are clamped.
	PipelineDepth int
	// StallTimeout is how long one block request may go unanswered before it
	// is re-issued to another peer. Zero means defaultStallTimeout.
	StallTimeout time.Duration
}

// Engine downloads a torrent's content from a swarm. One scheduler goroutine
// owns every piece of scheduling state; each connection runs its own read and
// write pumps and talks to the scheduler over channels. That keeps the
// pending-piece state single-sourced, which is what rules out two workers on
// the same piece and the "re-enqueue forever" spin.
type Engine struct {
	cfg   Config
	meta  *metainfo.MetaInfo
	store *storage.Storage

	pipelineDepth int
	stallTimeout  time.Duration
	stallTick     time.Duration

	logMu sync.Mutex

	mu         sync.Mutex
	pieces     []*pieceState
	pieceCount int
	have       []byte
	haveCount  int
	bytesDone  int64
	peers      map[string]*peerState
	blacklist  map[string]bool
	attempting map[string]bool
	counters   counters
	fatal      error

	lastAnnounce time.Time

	events chan event
	dials  chan tracker.Peer
	done   chan struct{}

	shutdownOnce sync.Once
	dialerWG     sync.WaitGroup
	dialWG       sync.WaitGroup
	peerWG       sync.WaitGroup
}

// New builds an engine for one download.
func New(cfg Config) *Engine {
	if cfg.PipelineDepth <= 0 {
		cfg.PipelineDepth = defaultPipelineDepth
	}
	if cfg.PipelineDepth > maxPipelineDepth {
		cfg.PipelineDepth = maxPipelineDepth
	}
	if cfg.StallTimeout <= 0 {
		cfg.StallTimeout = defaultStallTimeout
	}
	e := &Engine{
		cfg:           cfg,
		meta:          cfg.Meta,
		pipelineDepth: cfg.PipelineDepth,
		stallTimeout:  cfg.StallTimeout,
		peers:         make(map[string]*peerState),
		blacklist:     make(map[string]bool),
		attempting:    make(map[string]bool),
	}
	// The stall reaper runs on a tick; it is the shortest of a quarter of the
	// grace, 50 ms and one second, so expiry is never off by more than a tick
	// without waking a finished engine repeatedly.
	e.stallTick = cfg.StallTimeout / 4
	if e.stallTick < 50*time.Millisecond {
		e.stallTick = 50 * time.Millisecond
	}
	if e.stallTick > time.Second {
		e.stallTick = time.Second
	}
	return e
}

func (e *Engine) logf(format string, args ...any) {
	if e.cfg.Log == nil {
		return
	}
	e.logMu.Lock()
	e.cfg.Log(format, args...)
	e.logMu.Unlock()
}

// Run downloads every piece and returns once the content is complete.
func (e *Engine) Run(ctx context.Context) error {
	meta := e.meta
	if meta == nil {
		return errors.New("engine: no metainfo")
	}
	if meta.Multifile() {
		return errors.New("engine: multi-file torrents are not supported yet")
	}

	store, err := storage.Open(e.cfg.Output, meta.Info.PieceLength, meta.TotalLength())
	if err != nil {
		return fmt.Errorf("engine: open output %s: %w", e.cfg.Output, err)
	}
	defer store.Close()
	e.store = store

	e.pieceCount = meta.PieceCount()
	e.have = wire.NewBitfield(e.pieceCount)
	e.pieces = make([]*pieceState, e.pieceCount)
	for i := range e.pieces {
		e.pieces[i] = newPiece(i, meta.PieceSize(i))
	}
	e.events = make(chan event, eventBuffer)
	e.dials = make(chan tracker.Peer, dialBuffer)
	e.done = make(chan struct{})

	e.dialerWG.Add(1)
	go e.runDialer(ctx)
	defer e.shutdown()

	// A fatal reply here (a tracker that rejects our info-hash) is the one
	// announce failure worth stopping for; transient ones are retried below.
	resp, err := e.announce(ctx, tracker.EventStarted)
	if err != nil {
		if tracker.IsFatal(err) {
			return err
		}
		e.logf("engine: initial announce failed, will retry: %v", err)
	}

	if err := e.download(ctx, e.announceInterval(resp)); err != nil {
		return err
	}

	if _, err := e.announce(ctx, tracker.EventCompleted); err != nil {
		e.logf("engine: completed announce failed: %v", err)
	}

	s := e.Stats()
	e.logf("engine: complete: pieces %d/%d, bytes verified %d/%d, bytes in %d, blocks received %d, stalled %d, duplicate %d, bad pieces %d, peers used %d, max active peers %d",
		s.PiecesDone, s.Pieces, s.BytesDone, s.BytesTotal, s.BytesIn,
		s.BlocksReceived, s.BlocksStalled, s.BlocksDuplicate, s.BadPieces, s.PeersUsed, s.PeersMaxActive)
	return nil
}

// shutdown stops every goroutine the engine started and waits for them, so a
// returned Run leaves nothing touching the engine's state.
func (e *Engine) shutdown() {
	e.shutdownOnce.Do(func() {
		close(e.done)
		e.mu.Lock()
		for _, p := range e.peers {
			p.pc.close()
		}
		e.mu.Unlock()
	})
	// The dialer goroutine waits for its in-flight dials (which start the
	// connection pumps) before it returns, so no pump can be added after that
	// wait and the peerWG wait below is exhaustive.
	e.dialerWG.Wait()
	e.peerWG.Wait()
}

// download is the scheduler loop: it owns the pending-piece state, hands work
// to idle peers, and folds in everything the connection pumps report.
func (e *Engine) download(ctx context.Context, interval time.Duration) error {
	announce := time.NewTicker(interval)
	defer announce.Stop()
	reap := time.NewTicker(e.stallTick)
	defer reap.Stop()

	lastProgress := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		e.mu.Lock()
		fatal := e.fatal
		done := e.haveCount == e.pieceCount
		e.scheduleLocked()
		e.mu.Unlock()
		if fatal != nil {
			return fatal
		}
		if done {
			return nil
		}

		progressed := false
		select {
		case ev := <-e.events:
			e.mu.Lock()
			progressed = e.handleLocked(ev)
			e.mu.Unlock()
		case <-reap.C:
			e.mu.Lock()
			e.expireStallsLocked()
			starved := len(e.peers) == 0 && len(e.attempting) == 0 && time.Since(e.lastAnnounce) >= swarmRetryDelay
			e.mu.Unlock()
			if starved {
				e.reannounce(ctx)
			}
		case <-announce.C:
			e.reannounce(ctx)
		case <-ctx.Done():
			return ctx.Err()
		}

		if progressed {
			lastProgress = time.Now()
		}
		if time.Since(lastProgress) > noProgressTimeout {
			e.mu.Lock()
			got := e.bytesDone
			e.mu.Unlock()
			return fmt.Errorf("engine: no progress for %s (downloaded %d of %d bytes)",
				noProgressTimeout, got, e.meta.TotalLength())
		}
	}
}

func (e *Engine) reannounce(ctx context.Context) {
	if _, err := e.announce(ctx, ""); err != nil {
		e.logf("engine: announce failed: %v", err)
	}
}

// announce performs one announce round and queues any new peers for dialling.
func (e *Engine) announce(ctx context.Context, ev tracker.Event) (tracker.AnnounceResponse, error) {
	e.mu.Lock()
	done := e.bytesDone
	e.lastAnnounce = time.Now()
	e.mu.Unlock()

	req := tracker.AnnounceRequest{
		InfoHash:   e.meta.InfoHash,
		PeerID:     e.cfg.PeerID,
		Port:       e.cfg.Port,
		Downloaded: done,
		Left:       e.meta.TotalLength() - done,
		Event:      ev,
		NumWant:    50,
	}
	resp, err := tracker.AnnounceWithRetry(ctx, e.cfg.Tracker, req, tracker.DefaultRetry)
	if err != nil {
		return resp, err
	}
	e.queuePeers(ctx, resp.Peers)
	return resp, nil
}

// announceInterval clamps whatever the tracker asked for into a sane range.
func (e *Engine) announceInterval(resp tracker.AnnounceResponse) time.Duration {
	d := resp.Interval
	if d <= 0 {
		d = defaultAnnounceInterval
	}
	if d < minAnnounceInterval {
		d = minAnnounceInterval
	}
	if d > maxAnnounceInterval {
		d = maxAnnounceInterval
	}
	return d
}

// queuePeers asks the dialer for every peer we are not already talking to.
// Unreachable ports and anything blacklisted are skipped rather than retried.
func (e *Engine) queuePeers(ctx context.Context, peers []tracker.Peer) {
	for _, peer := range peers {
		if peer.Port == 0 || peer.IP == nil {
			continue
		}
		addr := peer.Addr()
		e.mu.Lock()
		skip := e.blacklist[addr] || e.attempting[addr]
		if !skip {
			e.attempting[addr] = true
		}
		e.mu.Unlock()
		if skip {
			continue
		}
		select {
		case e.dials <- peer:
		case <-ctx.Done():
			e.forgetAttempt(addr)
			return
		case <-e.done:
			e.forgetAttempt(addr)
			return
		}
	}
}

func (e *Engine) forgetAttempt(addr string) {
	e.mu.Lock()
	delete(e.attempting, addr)
	e.mu.Unlock()
}

// runDialer owns the outbound connections so a slow or dead peer never stalls
// the scheduler.
func (e *Engine) runDialer(ctx context.Context) {
	defer e.dialerWG.Done()
	defer e.dialWG.Wait()
	sem := make(chan struct{}, maxDialers)
	for {
		select {
		case <-e.done:
			return
		case <-ctx.Done():
			return
		case peer := <-e.dials:
			sem <- struct{}{}
			e.dialWG.Add(1)
			go func(p tracker.Peer) {
				defer e.dialWG.Done()
				defer func() { <-sem }()
				e.dialPeer(ctx, p)
			}(peer)
		}
	}
}

func (e *Engine) dialPeer(ctx context.Context, peer tracker.Peer) {
	addr := peer.Addr()

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout+handshakeTimeout)
	defer cancel()
	dialer := &net.Dialer{Timeout: dialTimeout}
	conn, err := dialer.DialContext(dialCtx, "tcp", addr)
	if err != nil {
		e.dialFailed(addr, err)
		return
	}
	if err := conn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		conn.Close()
		e.dialFailed(addr, err)
		return
	}
	if _, err := conn.Write(wire.NewHandshake(e.meta.InfoHash, e.cfg.PeerID).Encode()); err != nil {
		conn.Close()
		e.dialFailed(addr, err)
		return
	}
	hs, err := wire.ReadHandshake(conn, e.meta.InfoHash)
	if err != nil {
		conn.Close()
		e.dialFailed(addr, err)
		return
	}
	// The handshake deadline is per connection setup; the pumps set their own
	// per-operation deadlines from here on.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		conn.Close()
		e.dialFailed(addr, err)
		return
	}

	pc := newPeerConn(e, addr, conn, hs.PeerID)
	if !e.emit(event{kind: evConnected, peer: pc, addr: addr}) {
		conn.Close()
		return
	}
	e.peerWG.Add(2)
	go func() {
		defer e.peerWG.Done()
		pc.writeLoop()
	}()
	go func() {
		defer e.peerWG.Done()
		pc.readLoop()
	}()
}

// dialFailed releases the dial slot so a later announce can retry the peer.
func (e *Engine) dialFailed(addr string, err error) {
	e.forgetAttempt(addr)
	e.logf("engine: peer %s: %v", addr, err)
}

// emit hands an event to the scheduler without ever blocking past shutdown.
func (e *Engine) emit(ev event) bool {
	select {
	case e.events <- ev:
		return true
	case <-e.done:
		return false
	}
}

// peerGone is the single teardown path for a connection, whichever pump found
// the failure first.
func (e *Engine) peerGone(p *peerConn) {
	p.close()
	e.emit(event{kind: evGone, peer: p, addr: p.addr})
}
