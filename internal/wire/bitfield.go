package wire

// A bitfield carries one bit per piece, most-significant bit first within each
// byte. Bits past the last piece in the final byte are padding and stay zero.

// NewBitfield returns a zeroed bitfield for pieceCount pieces.
func NewBitfield(pieceCount int) []byte {
	if pieceCount <= 0 {
		return nil
	}
	return make([]byte, (pieceCount+7)/8)
}

// BitfieldHas reports whether the bit for index is set. An out-of-range index
// is absent rather than a panic.
func BitfieldHas(bf []byte, index int) bool {
	if index < 0 || index/8 >= len(bf) {
		return false
	}
	return bf[index/8]&(0x80>>uint(index%8)) != 0
}

// BitfieldSet marks index as present.
func BitfieldSet(bf []byte, index int) {
	if index < 0 || index/8 >= len(bf) {
		return
	}
	bf[index/8] |= 0x80 >> uint(index%8)
}

// BitfieldCount counts set bits, ignoring the padding past pieceCount.
func BitfieldCount(bf []byte, pieceCount int) int {
	n := 0
	for i := 0; i < pieceCount; i++ {
		if BitfieldHas(bf, i) {
			n++
		}
	}
	return n
}

// BitfieldComplete returns a bitfield with every piece present.
func BitfieldComplete(pieceCount int) []byte {
	bf := NewBitfield(pieceCount)
	for i := 0; i < pieceCount; i++ {
		BitfieldSet(bf, i)
	}
	return bf
}
