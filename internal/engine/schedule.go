package engine

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"time"

	"torrent-client/internal/wire"
)

// eventKind says what a connection pump reported to the scheduler.
type eventKind int

const (
	evConnected eventKind = iota
	evBitfield
	evHave
	evUnchoked
	evChoked
	evBlock
	evRequestDropped
	evGone
)

// event is one thing that happened on one connection. Everything a pump
// reports travels in this shape so the scheduler has a single input.
type event struct {
	kind  eventKind
	peer  *peerConn
	addr  string
	bits  []byte
	index int
	begin uint32
	data  []byte
}

// counters are the session totals behind Stats. They live under Engine.mu with
// the rest of the scheduling state.
type counters struct {
	bytesIn         int64
	blocksRequested int64
	blocksReceived  int64
	blocksDuplicate int64
	blocksStalled   int64
	badPieces       int64
	peersDropped    int64
	peersMaxActive  int
}

// handleLocked folds one event into the scheduling state and reports whether
// it moved the download forward.
func (e *Engine) handleLocked(ev event) bool {
	switch ev.kind {
	case evConnected:
		e.connectedLocked(ev)
		return false

	case evBitfield:
		p := e.peers[ev.addr]
		if p == nil {
			return false
		}
		if len(ev.bits) > len(p.bits) {
			e.dropPeerLocked(p, fmt.Sprintf("bitfield is %d bytes but the torrent has %d pieces", len(ev.bits), e.pieceCount))
			return false
		}
		copy(p.bits, ev.bits)
		e.refreshInterestLocked(p)
		e.scheduleLocked()
		return false

	case evHave:
		p := e.peers[ev.addr]
		if p == nil {
			return false
		}
		if ev.index < 0 || ev.index >= e.pieceCount {
			e.dropPeerLocked(p, fmt.Sprintf("have index %d is out of range for %d pieces", ev.index, e.pieceCount))
			return false
		}
		wire.BitfieldSet(p.bits, ev.index)
		e.refreshInterestLocked(p)
		e.scheduleLocked()
		return false

	case evUnchoked:
		p := e.peers[ev.addr]
		if p == nil {
			return false
		}
		p.choked = false
		e.scheduleLocked()
		return false

	case evChoked:
		p := e.peers[ev.addr]
		if p == nil {
			return false
		}
		p.choked = true
		// Outstanding blocks are put back on the market, but the piece keeps
		// whatever it already received, so a mid-transfer choke costs the
		// in-flight window, not the work.
		e.releasePeerLocked(p)
		e.scheduleLocked()
		return false

	case evBlock:
		p := e.peers[ev.addr]
		if p == nil {
			return false
		}
		progress := e.blockLocked(p, ev)
		e.scheduleLocked()
		return progress

	case evRequestDropped:
		// The write pump refused to send a request because the peer had choked
		// us in the meantime. Putting the block back immediately is what makes
		// a mid-transfer choke recover without waiting for the stall timer.
		p := e.peers[ev.addr]
		if p == nil {
			return false
		}
		if ev.index >= 0 && ev.index < len(e.pieces) {
			ps := e.pieces[ev.index]
			if i := ps.blockAt(ev.begin); i >= 0 && ps.blocks[i].inflight == p {
				ps.clearInflight(i)
			}
		}
		e.scheduleLocked()
		return false

	case evGone:
		e.goneLocked(ev.addr)
		return false
	}
	return false
}

func (e *Engine) connectedLocked(ev event) {
	if e.blacklist[ev.addr] {
		ev.peer.close()
		return
	}
	if _, ok := e.peers[ev.addr]; ok {
		// A second connection to the same peer buys nothing; keep the first.
		ev.peer.close()
		return
	}
	e.peers[ev.addr] = &peerState{
		pc:        ev.peer,
		addr:      ev.addr,
		peerID:    ev.peer.peerID,
		connected: true,
		choked:    true,
		piece:     -1,
		bits:      wire.NewBitfield(e.pieceCount),
	}
	e.logf("engine: connected to %s", ev.addr)
}

// goneLocked retires a connection that failed or closed. It is not a
// blacklist: a peer that goes away may come back on the next announce.
func (e *Engine) goneLocked(addr string) {
	p := e.peers[addr]
	if p == nil {
		return
	}
	delete(e.peers, addr)
	delete(e.attempting, addr)
	p.connected = false
	e.releasePeerLocked(p)
	// A piece that a departing peer had contributed to is thrown away: its
	// blocks may be bad, and blaming only the peers still connected on a later
	// hash failure would be wrong.
	e.forgetContributionsLocked(p)
	e.logf("engine: peer %s disconnected", addr)
}

// scheduleLocked hands work to every idle, unchoked peer. It is the only place
// a request is created, so the in-flight window is enforced in one spot: a
// block is claimed for the peer as the request is queued, which is what keeps
// a later pass from asking for the same block again.
func (e *Engine) scheduleLocked() {
	for _, p := range e.peers {
		if !p.connected || p.choked || p.blacklisted {
			continue
		}
		for p.inflight < e.pipelineDepth {
			ps, i, ok := e.nextBlockLocked(p)
			if !ok {
				break
			}
			ps.blocks[i].inflight = p
			ps.blocks[i].deadline = time.Now().Add(e.stallTimeout)
			p.inflight++
			e.counters.blocksRequested++
			p.pc.push(wire.Message{
				ID:     wire.IDRequest,
				Index:  uint32(ps.index),
				Begin:  ps.blocks[i].begin,
				Length: ps.blocks[i].length,
			})
		}
	}
	e.updateActiveLocked()
}

// nextBlockLocked picks the next block for one peer. A peer keeps working the
// piece it owns until that piece is done; otherwise it adopts the first piece
// it can actually serve. "It has the piece" is checked against the peer's
// bitfield, so a peer that lacks a piece is never enqueued for it — the
// classic idle-spin bug has no path here.
func (e *Engine) nextBlockLocked(p *peerState) (*pieceState, int, bool) {
	if p.piece >= 0 {
		ps := e.pieces[p.piece]
		if !ps.verified {
			if i := ps.nextMissing(); i >= 0 {
				return ps, i, true
			}
		}
		return nil, 0, false
	}
	for _, ps := range e.pieces {
		if ps.verified || ps.owner != nil {
			continue
		}
		if !wire.BitfieldHas(p.bits, ps.index) {
			continue
		}
		i := ps.nextMissing()
		if i < 0 {
			continue
		}
		ps.owner = p
		p.piece = ps.index
		return ps, i, true
	}
	return nil, 0, false
}

// blockLocked applies one block a peer sent us.
func (e *Engine) blockLocked(p *peerState, ev event) bool {
	if ev.index < 0 || ev.index >= len(e.pieces) {
		e.dropPeerLocked(p, fmt.Sprintf("block for piece %d is out of range", ev.index))
		return false
	}
	ps := e.pieces[ev.index]
	if ps.verified {
		return false
	}
	i := ps.blockAt(ev.begin)
	if i < 0 {
		e.dropPeerLocked(p, fmt.Sprintf("unrequested block at %d of piece %d", ev.begin, ev.index))
		return false
	}
	b := &ps.blocks[i]
	if b.received {
		e.counters.blocksDuplicate++
		return false
	}
	if uint32(len(ev.data)) != b.length {
		e.dropPeerLocked(p, fmt.Sprintf("block %d of piece %d is %d bytes, want %d", ev.begin, ev.index, len(ev.data), b.length))
		return false
	}

	if ps.buf == nil {
		ps.buf = make([]byte, ps.size)
	}
	copy(ps.buf[b.begin:], ev.data)
	ps.clearInflight(i)
	b.received = true
	ps.received++
	ps.contributors[p] = true
	p.served++
	e.counters.blocksReceived++
	e.counters.bytesIn += int64(len(ev.data))
	e.updateActiveLocked()

	if ps.received == len(ps.blocks) {
		e.verifyPieceLocked(ps)
	}
	return true
}

// verifyPieceLocked checks a completed piece and either keeps it or blames the
// peers that supplied it.
func (e *Engine) verifyPieceLocked(ps *pieceState) {
	if ps.owner != nil {
		ps.owner.piece = -1
	}
	contributors := make([]*peerState, 0, len(ps.contributors))
	for p := range ps.contributors {
		contributors = append(contributors, p)
	}

	sum := sha1.Sum(ps.buf)
	if !bytes.Equal(sum[:], e.meta.PieceHash(ps.index)) {
		e.counters.badPieces++
		e.logf("engine: piece %d failed SHA-1 (%d contributor(s))", ps.index, len(contributors))
		ps.reset()
		for _, p := range contributors {
			e.dropPeerLocked(p, fmt.Sprintf("served data that failed verification for piece %d", ps.index))
		}
		return
	}

	if err := e.store.WritePiece(ps.index, ps.buf); err != nil {
		// A storage failure cannot be retried away, so it ends the download.
		e.fatal = fmt.Errorf("engine: write piece %d: %w", ps.index, err)
		ps.reset()
		return
	}

	ps.verified = true
	ps.buf = nil
	ps.contributors = nil
	wire.BitfieldSet(e.have, ps.index)
	e.haveCount++
	e.bytesDone += ps.size
	e.logf("engine: piece %d/%d verified (%d/%d bytes)", ps.index+1, e.pieceCount, e.bytesDone, e.meta.TotalLength())

	for _, p := range e.peers {
		if p.connected {
			p.pc.broadcastHave(ps.index)
		}
	}
}

// releasePeerLocked takes a peer out of the request loop without forgetting
// what it already gave us: its piece is reopened for any other peer to adopt,
// and its outstanding requests are cleared so the stall reaper is not needed.
func (e *Engine) releasePeerLocked(p *peerState) {
	for _, ps := range e.pieces {
		if ps.owner == p {
			ps.owner = nil
		}
		for i := range ps.blocks {
			if ps.blocks[i].inflight == p {
				ps.blocks[i].inflight = nil
			}
		}
	}
	p.piece = -1
	p.inflight = 0
}

// forgetContributionsLocked drops the partial state of every unfinished piece
// this peer had a hand in, so a later hash failure can never blame a peer that
// is still connected for data a departed peer supplied.
func (e *Engine) forgetContributionsLocked(p *peerState) {
	for _, ps := range e.pieces {
		if !ps.verified && ps.contributors[p] {
			ps.reset()
		}
	}
}

// dropPeerLocked blacklists a peer for the session and tears its connection
// down. It is the response to bad data and to protocol violations.
func (e *Engine) dropPeerLocked(p *peerState, reason string) {
	if p.blacklisted {
		return
	}
	p.blacklisted = true
	e.blacklist[p.addr] = true
	delete(e.peers, p.addr)
	delete(e.attempting, p.addr)
	p.connected = false
	e.releasePeerLocked(p)
	e.forgetContributionsLocked(p)
	e.counters.peersDropped++
	e.logf("engine: dropping peer %s: %s", p.addr, reason)
	p.pc.close()
}

// refreshInterestLocked tells a peer we want something from it, once.
func (e *Engine) refreshInterestLocked(p *peerState) {
	if p.interested {
		return
	}
	for _, ps := range e.pieces {
		if !ps.verified && wire.BitfieldHas(p.bits, ps.index) {
			p.interested = true
			p.pc.push(wire.Message{ID: wire.IDInterested})
			return
		}
	}
}

// expireStallsLocked re-issues blocks whose requests went unanswered. The
// piece is handed back at the same time, so the block can go to a different
// peer instead of the one that is stalling.
func (e *Engine) expireStallsLocked() {
	now := time.Now()
	expired := false
	for _, ps := range e.pieces {
		if ps.verified {
			continue
		}
		for i := range ps.blocks {
			b := &ps.blocks[i]
			if b.inflight == nil || now.Before(b.deadline) {
				continue
			}
			owner := b.inflight
			ps.clearInflight(i)
			owner.stalls++
			e.counters.blocksStalled++
			expired = true
			if ps.owner == owner {
				ps.owner = nil
				owner.piece = -1
			}
		}
	}
	if expired {
		e.scheduleLocked()
	}
}

// updateActiveLocked keeps the peak number of peers with work in the pipe,
// which is the value that proves several peers were used at once.
func (e *Engine) updateActiveLocked() {
	active := 0
	for _, p := range e.peers {
		if p.window() > 0 {
			active++
		}
	}
	if active > e.counters.peersMaxActive {
		e.counters.peersMaxActive = active
	}
}
