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

// peerState is the scheduler's view of one peer. Every field here is touched
// only by the scheduler goroutine, under Engine.mu.
type peerState struct {
	pc     *peerConn
	addr   string
	peerID [20]byte

	connected  bool
	choked     bool
	interested bool
	// Excluded for the rest of the session.
	blacklisted bool
	bits        []byte

	// The piece this peer currently owns, or -1. One piece at a time is what
	// makes "no two workers on a piece" hold.
	piece int
	// Blocks handed to this peer and not yet answered, expired or released:
	// the bounded window that backpressures it, claimed when the request is
	// queued rather than written, so a second pass can't re-ask the same block.
	inflight int
	// Blocks this peer delivered, including ones a later hash check threw away.
	served int
	stalls int
}

// Number of block requests this peer has in the pipe.
func (p *peerState) window() int { return p.inflight }

// peerConn is one connection's I/O: a read loop, a write loop, and the queue
// between the scheduler and the wire. It decides nothing about what to
// download — it executes what the scheduler pushes and reports back.
type peerConn struct {
	eng  *Engine
	addr string
	conn net.Conn
	r    *bufio.Reader

	// The id the remote sent in its handshake, kept for stats.
	peerID [20]byte
	// Mirrors the last choke/unchoke, so the write pump can refuse to send
	// requests into a choke without asking the scheduler.
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

// push queues a request. The scheduler only pushes while the peer's window has
// room, so the queue never grows without bound.
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

// broadcastHave records a verified piece for the next write pass. Haves go into
// a list rather than the message queue, so a slow peer can't grow the
// scheduler's queue by one entry per finished piece.
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

// Tears the connection down once, whichever pump noticed first.
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

// readLoop is this connection's message pump. Bitfield and have go to the
// scheduler so it can judge the peer; choke state is folded in here too,
// because it belongs to the connection rather than any worker.
//
// The download cap is enforced here, where a block is accepted. Deliberate:
// requests keep a peer's window full, so pacing the asking would drain the
// pipeline and turn every block into a damn round-trip; pacing the intake
// keeps the pipe full and merely slows the transfer. Blocking this loop
// backpressures the wire over TCP, so the peer slows down instead of us
// buffering without bound.
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
			// The wait takes the engine's context, so a cancelled download
			// returns at once and the pump tears down instead of leaking.
			if err := p.eng.limiter.Wait(ctx, len(m.Block)); err != nil {
				return
			}
			if !p.eng.emit(event{kind: evBlock, peer: p, addr: p.addr, index: int(m.Index), begin: m.Begin, data: m.Block}) {
				return
			}
		default:
			// Keep-alives, our ids echoed back, and extension ids we don't
			// implement: all safe to ignore.
		}
	}
}

// writeLoop drains the outbound queue. A request that lands while the peer has
// choked us is reported as dropped instead of written, so the block goes back
// on the market at once rather than waiting out the stall timer.
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
