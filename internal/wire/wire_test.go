package wire

import (
	"bytes"
	"errors"
	"testing"
)

func sampleHandshake() ([20]byte, [20]byte, Handshake) {
	var infoHash, peerID [20]byte
	for i := range infoHash {
		infoHash[i] = byte(i)
	}
	copy(peerID[:], "-TC0001-abcdefghijkl")
	return infoHash, peerID, NewHandshake(infoHash, peerID)
}

func TestHandshakeEncodingIsExactly68Bytes(t *testing.T) {
	_, _, h := sampleHandshake()
	enc := h.Encode()

	if len(enc) != HandshakeLen {
		t.Fatalf("handshake is %d bytes, want %d", len(enc), HandshakeLen)
	}
	if enc[0] != 19 || string(enc[1:20]) != Protocol {
		t.Fatalf("pstrlen/pstr = %d/%q", enc[0], enc[1:20])
	}
	if !bytes.Equal(enc[20:28], make([]byte, 8)) {
		t.Fatalf("reserved bytes = %x, want zeroes", enc[20:28])
	}
}

func TestReadHandshakeRoundTrip(t *testing.T) {
	infoHash, peerID, h := sampleHandshake()

	got, err := ReadHandshake(bytes.NewReader(h.Encode()), infoHash)
	if err != nil {
		t.Fatalf("ReadHandshake: %v", err)
	}
	if got.InfoHash != infoHash || got.PeerID != peerID {
		t.Fatalf("handshake = %+v, want info-hash %x peer id %x", got, infoHash, peerID)
	}
	if got.Reserved != [8]byte{} {
		t.Fatalf("reserved = %x, want zeroes", got.Reserved)
	}
}

func TestReadHandshakeRejectsWrongInfoHash(t *testing.T) {
	infoHash, _, h := sampleHandshake()
	other := infoHash
	other[0] ^= 0xff

	if _, err := ReadHandshake(bytes.NewReader(h.Encode()), other); !errors.Is(err, ErrInfoHashMismatch) {
		t.Fatalf("err = %v, want ErrInfoHashMismatch", err)
	}
}

func TestReadHandshakeRejectsWrongProtocol(t *testing.T) {
	infoHash, _, h := sampleHandshake()
	enc := h.Encode()
	copy(enc[1:20], "NotBitTorrentProtcl") // same length, wrong string

	if _, err := ReadHandshake(bytes.NewReader(enc), infoHash); !errors.Is(err, ErrBadProtocol) {
		t.Fatalf("err = %v, want ErrBadProtocol", err)
	}
}

func TestReadHandshakeRejectsTruncatedFrame(t *testing.T) {
	infoHash, _, h := sampleHandshake()
	if _, err := ReadHandshake(bytes.NewReader(h.Encode()[:40]), infoHash); err == nil {
		t.Fatal("a 40-byte handshake was accepted")
	}
}

func TestMessageRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  Message
		same func(a, b Message) bool
	}{
		{
			name: "keep-alive",
			msg:  Message{ID: IDKeepAlive},
			same: func(a, b Message) bool { return a.ID == IDKeepAlive && b.ID == IDKeepAlive },
		},
		{"choke", Message{ID: IDChoke}, sameHeader},
		{"unchoke", Message{ID: IDUnchoke}, sameHeader},
		{"interested", Message{ID: IDInterested}, sameHeader},
		{"not_interested", Message{ID: IDNotInterested}, sameHeader},
		{"have", Message{ID: IDHave, Index: 7}, sameHeader},
		{
			name: "request",
			msg:  Message{ID: IDRequest, Index: 3, Begin: 16384, Length: 16384},
			same: sameHeader,
		},
		{
			name: "cancel",
			msg:  Message{ID: IDCancel, Index: 3, Begin: 16384, Length: 16384},
			same: sameHeader,
		},
		{
			name: "piece",
			msg:  Message{ID: IDPiece, Index: 2, Begin: 32768, Block: []byte("block bytes")},
			same: func(a, b Message) bool {
				return sameHeader(a, b) && bytes.Equal(a.Block, b.Block)
			},
		},
		{
			name: "bitfield",
			msg:  Message{ID: IDBitfield, Bitfield: []byte{0x80, 0x40}},
			same: func(a, b Message) bool {
				return sameHeader(a, b) && bytes.Equal(a.Bitfield, b.Bitfield)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame, err := tt.msg.Encode()
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := Decode(bytes.NewReader(frame))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !tt.same(tt.msg, got) {
				t.Fatalf("round trip = %+v, want %+v", got, tt.msg)
			}
			// A second read of the same stream must be empty, which catches a
			// length prefix that does not match the payload.
			if _, err := Decode(bytes.NewReader(frame)); err != nil {
				t.Fatalf("re-decode: %v", err)
			}
		})
	}
}

func sameHeader(a, b Message) bool {
	return a.ID == b.ID && a.Index == b.Index && a.Begin == b.Begin && a.Length == b.Length
}

func TestKeepAliveIsAnEmptyFrame(t *testing.T) {
	frame, err := Message{ID: IDKeepAlive}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(frame, []byte{0, 0, 0, 0}) {
		t.Fatalf("keep-alive frame = %x, want 00000000", frame)
	}
}

func TestDecodeRejectsOversizedFrame(t *testing.T) {
	// A length prefix alone, a few bytes over the cap: the payload is never read.
	n := uint32(MaxMessageLength) + 8
	frame := []byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
	if _, err := Decode(bytes.NewReader(frame)); err == nil {
		t.Fatal("an oversized frame was accepted")
	}
}

func TestDecodeRejectsMalformedPayloads(t *testing.T) {
	tests := []struct {
		name  string
		frame []byte
	}{
		{"have with 3 bytes", []byte{0, 0, 0, 5, IDHave, 1, 2, 3}},
		{"request with 8 bytes", []byte{0, 0, 0, 9, IDRequest, 0, 0, 0, 1, 0, 0, 0, 2}},
		{"piece with 5 bytes", []byte{0, 0, 0, 6, IDPiece, 0, 0, 0, 1, 2}},
		{"choke with a payload", []byte{0, 0, 0, 2, IDChoke, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Decode(bytes.NewReader(tt.frame)); err == nil {
				t.Fatalf("Decode(%s) succeeded, want an error", tt.name)
			}
		})
	}
}

// The id space is reserved for extensions, so an unknown id must stay
// skippable rather than killing the connection.
func TestDecodeKeepsUnknownMessageIDs(t *testing.T) {
	frame := []byte{0, 0, 0, 3, 200, 0xaa, 0xbb}
	got, err := Decode(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.ID != 200 {
		t.Fatalf("id = %d, want 200", got.ID)
	}
}

func TestEncodeRejectsUnknownMessageID(t *testing.T) {
	if _, err := (Message{ID: 42}).Encode(); err == nil {
		t.Fatal("encoding an unknown message id succeeded")
	}
}

func TestRequestPayloadFieldOrderIsIndexBeginLength(t *testing.T) {
	frame, err := Message{ID: IDRequest, Index: 0x01020304, Begin: 0x05060708, Length: 0x090a0b0c}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0, 0, 0, 13, IDRequest, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	if !bytes.Equal(frame, want) {
		t.Fatalf("request frame = %x, want %x", frame, want)
	}
}

func TestPiecePayloadKeepsIndexAndBeginBigEndian(t *testing.T) {
	frame, err := Message{ID: IDPiece, Index: 1, Begin: 2, Block: []byte{0xde, 0xad}}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0, 0, 0, 11, IDPiece, 0, 0, 0, 1, 0, 0, 0, 2, 0xde, 0xad}
	if !bytes.Equal(frame, want) {
		t.Fatalf("piece frame = %x, want %x", frame, want)
	}
}

func TestBitfieldBitsAreMostSignificantFirst(t *testing.T) {
	bf := NewBitfield(10)
	BitfieldSet(bf, 0)
	BitfieldSet(bf, 7)
	BitfieldSet(bf, 8)

	if !bytes.Equal(bf, []byte{0x81, 0x80}) {
		t.Fatalf("bitfield = %x, want 8180", bf)
	}
	for _, i := range []int{0, 7, 8} {
		if !BitfieldHas(bf, i) {
			t.Fatalf("piece %d should be set", i)
		}
	}
	for _, i := range []int{1, 6, 9, 10, -1} {
		if BitfieldHas(bf, i) {
			t.Fatalf("piece %d should be unset", i)
		}
	}
}

func TestBitfieldPaddingBitsStayZero(t *testing.T) {
	bf := BitfieldComplete(10)
	if !bytes.Equal(bf, []byte{0xff, 0xc0}) {
		t.Fatalf("complete bitfield = %x, want ffc0", bf)
	}
	if got := BitfieldCount(bf, 10); got != 10 {
		t.Fatalf("BitfieldCount = %d, want 10", got)
	}
}

func TestBitfieldIgnoresOutOfRangeWrites(t *testing.T) {
	bf := NewBitfield(8)
	BitfieldSet(bf, 8)
	BitfieldSet(bf, -1)
	if !bytes.Equal(bf, []byte{0}) {
		t.Fatalf("out-of-range writes changed the bitfield: %x", bf)
	}
	if bf := NewBitfield(0); bf != nil {
		t.Fatalf("piece count 0 = %x, want nil", bf)
	}
}

func TestWriteProducesAFullFrame(t *testing.T) {
	var buf bytes.Buffer
	if err := Write(&buf, Message{ID: IDInterested}); err != nil {
		t.Fatal(err)
	}
	if got := buf.Bytes(); !bytes.Equal(got, []byte{0, 0, 0, 1, IDInterested}) {
		t.Fatalf("interested frame = %x", got)
	}
}
