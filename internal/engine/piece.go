package engine

import (
	"time"

	"torrent-client/internal/wire"
)

// blockRange is one request-sized slice of a piece. It is the unit the wire
// protocol and the per-connection request ledger both work in.
type blockRange struct {
	begin  uint32
	length uint32
}

// blockRanges splits a piece into requests of at most wire.BlockSize. The
// final block is short when the piece does not divide evenly, which is also
// how the short last piece of a torrent is handled.
func blockRanges(size int64) []blockRange {
	if size <= 0 {
		return nil
	}
	ranges := make([]blockRange, 0, (size+wire.BlockSize-1)/wire.BlockSize)
	for begin := int64(0); begin < size; begin += wire.BlockSize {
		length := int64(wire.BlockSize)
		if begin+length > size {
			length = size - begin
		}
		ranges = append(ranges, blockRange{begin: uint32(begin), length: uint32(length)})
	}
	return ranges
}

// block is one block's assembly state inside a piece.
type block struct {
	begin    uint32
	length   uint32
	received bool
	// inflight names the peer that owes us this block, or nil when nobody
	// does. Only the piece's owner may be named here, which is what stops two
	// workers fetching the same block.
	inflight *peerState
	// deadline is when an unanswered request for this block expires.
	deadline time.Time
}

// pieceState is one piece's assembly state. Only the scheduler goroutine
// touches it, under Engine.mu; the connection pumps never see it.
type pieceState struct {
	index  int
	size   int64
	blocks []block
	// buf holds the blocks received so far. It is kept across a handover so a
	// choke does not throw away verified-so-far work, and dropped on a failed
	// hash so a corrupt block can never survive a retry.
	buf          []byte
	received     int
	owner        *peerState
	verified     bool
	contributors map[*peerState]bool
}

func newPiece(index int, size int64) *pieceState {
	ps := &pieceState{index: index, size: size, contributors: make(map[*peerState]bool)}
	for _, r := range blockRanges(size) {
		ps.blocks = append(ps.blocks, block{begin: r.begin, length: r.length})
	}
	return ps
}

// blockAt maps a requested begin offset to a block index, or -1 when the
// offset is not one we asked for. Blocks are laid out every BlockSize bytes,
// so the mapping is arithmetic with a check against a hostile peer's offset.
func (ps *pieceState) blockAt(begin uint32) int {
	i := int(begin / wire.BlockSize)
	if i < 0 || i >= len(ps.blocks) || ps.blocks[i].begin != begin {
		return -1
	}
	return i
}

// nextMissing returns the index of the first block that is neither received
// nor already requested, or -1 when the piece has nothing more to ask for.
func (ps *pieceState) nextMissing() int {
	for i := range ps.blocks {
		if !ps.blocks[i].received && ps.blocks[i].inflight == nil {
			return i
		}
	}
	return -1
}

// clearInflight drops an outstanding request and keeps the owner's window
// accounting in step with the block table.
func (ps *pieceState) clearInflight(i int) {
	if p := ps.blocks[i].inflight; p != nil {
		ps.blocks[i].inflight = nil
		if p.inflight > 0 {
			p.inflight--
		}
	}
}

// reset throws the piece back to "nothing fetched". It is the recovery path
// for a failed hash: keeping any of the bytes could keep a corrupt block
// alive across retries, so none of them survive.
func (ps *pieceState) reset() {
	for i := range ps.blocks {
		ps.blocks[i].received = false
		ps.clearInflight(i)
	}
	ps.buf = nil
	ps.received = 0
	ps.owner = nil
	ps.contributors = make(map[*peerState]bool)
}
