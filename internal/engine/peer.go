package engine

import (
	"bufio"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"torrent-client/internal/wire"
)

// peerState is the scheduler's view of one peer. Every field is touched only
// by the scheduler goroutine under Engine.mu.
type peerState struct {
	pc     *peerConn
	addr   string
	peerID [20]byte

	connected  bool
	choked     bool
	interested bool
	// blacklisted peers are excluded for the rest of the session.
	blacklisted bool
	bits        []byte

	// piece is the piece this peer currently owns, or -1. A peer works on one
	// piece at a time, which is what makes "no two workers on a piece" hold.
	piece int
	// inflight counts blocks handed to this peer and not yet answered, expired
	// or released. It is the bounded in-flight window that backpressures the
	// peer, and it is claimed when the request is queued, not when it is
	// written, so a second scheduler pass cannot re-ask for the same block.
	inflight int
	// served counts blocks this peer delivered, including ones a later hash
	// check threw away.
	served int
	stalls int
}

// window is the number of block requests this peer has in the pipe.
func (p *peerState) window() int { return p.inflight }

// peerConn is one connection's I/O: a read loop, a write loop, and the queue
// between the scheduler and the wire. It decides nothing about what to
// download; it only executes what the scheduler pushes and reports back.
type peerConn struct {
	eng  *Engine
	addr string
	conn net.Conn
	r    *bufio.Reader

	// peerID is the id the remote sent in its handshake, kept for stats.
	peerID [20]byte
	// choked mirrors the last choke/unchoke so the write pump can refuse to
	// send requests into a choke without asking the scheduler.
	choked atomic.Bool

	mu    sync.Mutex
	queue []wire.Message
	haves []int
	dead  bool

	woke chan struct{}
	stop chan struct{}
	once sync.Once
}

func newPeerConn(eng *Engine, addr string, conn net.Conn, peerID [20]byte) *peerConn {
	p := &peerConn{
		eng:    eng,
		addr:   addr,
		conn:   conn,
		r:      bufio.NewReader(conn),
		peerID: peerID,
		woke:   make(chan struct{}, 1),
		stop:   make(chan struct{}),
	}
	p.choked.Store(true)
	return p
}

// push queues a request. The scheduler only pushes while the peer's window
// has room, so this never grows without bound.
func (p *peerConn) push(m wire.Message) {
	p.mu.Lock()
	if p.dead {
		p.mu.Unlock()
		return
	}
	p.queue = append(p.queue, m)
	p.mu.Unlock()
	p.wake()
}

// broadcastHave records a verified piece for the next write pass. Haves are
// coalesced into a list rather than queued as messages, so a slow peer cannot
// make the scheduler's queue grow with one entry per finished piece.
func (p *peerConn) broadcastHave(index int) {
	p.mu.Lock()
	if p.dead {
		p.mu.Unlock()
		return
	}
	p.haves = append(p.haves, index)
	p.mu.Unlock()
	p.wake()
}

func (p *peerConn) wake() {
	select {
	case p.woke <- struct{}{}:
	default:
	}
}

func (p *peerConn) take() ([]wire.Message, []int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	msgs, haves := p.queue, p.haves
	p.queue, p.haves = nil, nil
	return msgs, haves
}

// close tears down the connection once, whichever pump noticed first.
func (p *peerConn) close() {
	p.once.Do(func() {
		p.mu.Lock()
		p.dead = true
		p.queue, p.haves = nil, nil
		p.mu.Unlock()
		close(p.stop)
		p.conn.Close()
	})
}

// readLoop is this connection's message pump. Bitfield/have are forwarded so
// the scheduler can decide what the peer is worth; choke state is folded in
// here as well, because it belongs to the connection, not to any worker.
//
// The download cap is enforced here, at the point a block is accepted. Shaping
// accepted bytes rather than pacing the request loop is the deliberate choice:
// requests are what keep a peer's window full, so throttling the asking would
// drain the pipeline and collapse throughput into a round-trip per block,
// while throttling the intake leaves the pipe full and the transfer merely
// paced. Blocking this loop also backpressures the wire through TCP, so a
// peer is told to slow down rather than the client buffering without bound.
func (p *peerConn) readLoop(ctx context.Context) {
	defer p.eng.peerGone(p)
	for {
		if err := p.conn.SetReadDeadline(time.Now().Add(idleTimeout)); err != nil {
			return
		}
		m, err := wire.Decode(p.r)
		if err != nil {
			return
		}
		switch m.ID {
		case wire.IDBitfield:
			if !p.eng.emit(event{kind: evBitfield, peer: p, addr: p.addr, bits: m.Bitfield}) {
				return
			}
		case wire.IDHave:
			if !p.eng.emit(event{kind: evHave, peer: p, addr: p.addr, index: int(m.Index)}) {
				return
			}
		case wire.IDChoke:
			p.choked.Store(true)
			if !p.eng.emit(event{kind: evChoked, peer: p, addr: p.addr}) {
				return
			}
		case wire.IDUnchoke:
			p.choked.Store(false)
			if !p.eng.emit(event{kind: evUnchoked, peer: p, addr: p.addr}) {
				return
			}
		case wire.IDPiece:
			// A cancelled download must not sit here: the wait takes the
			// engine's context and returns as soon as it is done, which is
			// what tears the pump down instead of leaking a goroutine.
			if err := p.eng.limiter.Wait(ctx, len(m.Block)); err != nil {
				return
			}
			if !p.eng.emit(event{kind: evBlock, peer: p, addr: p.addr, index: int(m.Index), begin: m.Begin, data: m.Block}) {
				return
			}
		default:
			// Keep-alives, our own ids echoed back, and extension ids we do
			// not implement are all safe to ignore.
		}
	}
}

// writeLoop drains the outbound queue. A request that would land while the
// peer has choked us is reported as dropped instead of written, so the block
// goes back on the market immediately rather than waiting for the stall timer.
func (p *peerConn) writeLoop() {
	for {
		msgs, haves := p.take()
		for _, m := range msgs {
			if m.ID == wire.IDRequest && p.choked.Load() {
				p.eng.emit(event{kind: evRequestDropped, peer: p, addr: p.addr, index: int(m.Index), begin: m.Begin})
				continue
			}
			if err := p.write(m); err != nil {
				p.eng.peerGone(p)
				return
			}
		}
		for _, index := range haves {
			if err := p.write(wire.Message{ID: wire.IDHave, Index: uint32(index)}); err != nil {
				p.eng.peerGone(p)
				return
			}
		}
		select {
		case <-p.woke:
		case <-p.stop:
			return
		case <-p.eng.done:
			return
		}
	}
}

func (p *peerConn) write(m wire.Message) error {
	if err := p.conn.SetWriteDeadline(time.Now().Add(idleTimeout)); err != nil {
		return err
	}
	return wire.Write(p.conn, m)
}
