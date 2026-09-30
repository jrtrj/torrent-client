package engine

import (
	"testing"

	"torrent-client/internal/wire"
)

// The block splitter is the piece-boundary math: every block is at most 16 KiB
// and only the final one of a piece is short.
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

func TestFindBlock(t *testing.T) {
	blocks := blockRanges(2 * wire.BlockSize)
	if got := findBlock(blocks, wire.BlockSize); got == nil || got.begin != wire.BlockSize {
		t.Fatalf("findBlock(%d) = %+v", wire.BlockSize, got)
	}
	if got := findBlock(blocks, 5); got != nil {
		t.Fatalf("findBlock(5) = %+v, want nil", got)
	}
}
