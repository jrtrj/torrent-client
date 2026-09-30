package engine

import (
	"testing"

	"torrent-client/internal/wire"
)

// Blocks are capped at 16 KiB; only a piece's final block may be short.
func TestBlockRanges(t *testing.T) {
	tests := []struct {
		name string
		size int64
		want [][2]uint32
	}{
		{"empty", 0, nil},
		{"one short block", 1, [][2]uint32{{0, 1}}},
		{"exactly one block", wire.BlockSize, [][2]uint32{{0, wire.BlockSize}}},
		{"block plus one", wire.BlockSize + 1, [][2]uint32{{0, wire.BlockSize}, {wire.BlockSize, 1}}},
		{"three blocks", 3 * wire.BlockSize, [][2]uint32{{0, wire.BlockSize}, {wire.BlockSize, wire.BlockSize}, {2 * wire.BlockSize, wire.BlockSize}}},
		{"ragged tail", 2*wire.BlockSize + 7, [][2]uint32{{0, wire.BlockSize}, {wire.BlockSize, wire.BlockSize}, {2 * wire.BlockSize, 7}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := blockRanges(tt.size)
			if len(got) != len(tt.want) {
				t.Fatalf("blockRanges(%d) has %d blocks, want %d", tt.size, len(got), len(tt.want))
			}
			var covered int64
			for i, b := range got {
				if b.begin != tt.want[i][0] || b.length != tt.want[i][1] {
					t.Fatalf("block %d = (%d,%d), want (%d,%d)", i, b.begin, b.length, tt.want[i][0], tt.want[i][1])
				}
				if b.length > wire.BlockSize {
					t.Fatalf("block %d is %d bytes, over the %d cap", i, b.length, wire.BlockSize)
				}
				covered += int64(b.length)
			}
			if covered != tt.size {
				t.Fatalf("blocks cover %d bytes, want %d", covered, tt.size)
			}
		})
	}
}

func TestBlockRangesAreContiguous(t *testing.T) {
	blocks := blockRanges(5 * wire.BlockSize / 2)
	for i := 1; i < len(blocks); i++ {
		if want := blocks[i-1].begin + blocks[i-1].length; blocks[i].begin != want {
			t.Fatalf("block %d begins at %d, want %d", i, blocks[i].begin, want)
		}
	}
}

func TestPieceBlockLookup(t *testing.T) {
	ps := newPiece(0, 2*wire.BlockSize+7)
	if got := ps.blockAt(wire.BlockSize); got != 1 {
		t.Fatalf("blockAt(%d) = %d, want 1", wire.BlockSize, got)
	}
	if got := ps.blockAt(5); got != -1 {
		t.Fatalf("blockAt(5) = %d, want -1", got)
	}
	if got := ps.blockAt(2 * wire.BlockSize); got != 2 {
		t.Fatalf("blockAt(%d) = %d, want the short tail block", 2*wire.BlockSize, got)
	}
	if got := ps.blockAt(3 * wire.BlockSize); got != -1 {
		t.Fatalf("blockAt(%d) = %d, want -1 past the end", 3*wire.BlockSize, got)
	}
}

// A piece is only "missing" a block nobody holds and nobody was asked for, so
// the scheduler cannot hand the same block to two peers.
func TestPieceNextMissing(t *testing.T) {
	ps := newPiece(0, 3*wire.BlockSize)
	if got := ps.nextMissing(); got != 0 {
		t.Fatalf("nextMissing() = %d, want 0", got)
	}
	owner := &peerState{piece: 0}
	ps.blocks[0].received = true
	ps.blocks[1].inflight = owner
	if got := ps.nextMissing(); got != 2 {
		t.Fatalf("nextMissing() = %d, want 2", got)
	}
	ps.blocks[2].received = true
	if got := ps.nextMissing(); got != -1 {
		t.Fatalf("nextMissing() = %d, want -1 when nothing is askable", got)
	}
}

func TestPieceResetClearsInflightAccounting(t *testing.T) {
	ps := newPiece(0, 2*wire.BlockSize)
	owner := &peerState{piece: 0, inflight: 1}
	ps.blocks[0].received = true
	ps.blocks[1].inflight = owner
	ps.received = 1
	ps.buf = make([]byte, ps.size)

	ps.reset()

	if ps.received != 0 || ps.buf != nil || ps.owner != nil {
		t.Fatalf("reset left received=%d buf=%v owner=%v", ps.received, ps.buf, ps.owner)
	}
	if owner.inflight != 0 {
		t.Fatalf("reset left the owner with %d in flight, want 0", owner.inflight)
	}
	for i := range ps.blocks {
		if ps.blocks[i].received || ps.blocks[i].inflight != nil {
			t.Fatalf("block %d survived reset: %+v", i, ps.blocks[i])
		}
	}
}
