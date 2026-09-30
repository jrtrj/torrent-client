package wire

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Message ids, as fixed by BEP 3.
const (
	IDChoke         byte = 0
	IDUnchoke       byte = 1
	IDInterested    byte = 2
	IDNotInterested byte = 3
	IDHave          byte = 4
	IDBitfield      byte = 5
	IDRequest       byte = 6
	IDPiece         byte = 7
	IDCancel        byte = 8
	// IDExtended is the extension protocol (BEP 10). Its payload is a one-byte
	// extension message id followed by that extension's own body.
	IDExtended byte = 20
)

// IDKeepAlive is not a wire id: a zero length prefix carries no id byte at
// all, so this sentinel lets callers handle keep-alives in the same switch as
// real messages.
const IDKeepAlive byte = 0xff

// Message is a decoded peer message. Only the fields meaningful for ID are
// populated; everything else stays zero.
//
//   - Choke/Unchoke/Interested/NotInterested/KeepAlive: no payload
//   - Have: Index
//   - Bitfield: Bitfield
//   - Request/Cancel: Index, Begin, Length
//   - Piece: Index, Begin, Block
type Message struct {
	ID       byte
	Index    uint32
	Begin    uint32
	Length   uint32
	Block    []byte
	Bitfield []byte
	// Extended is the payload of an IDExtended message: a one-byte extension
	// message id, then that extension's body. For a ut_metadata data message
	// the body is a bencoded dictionary immediately followed by the raw
	// metadata bytes, which is why it is kept as one opaque slice here.
	Extended []byte
}

// Encode renders the full frame, length prefix included.
func (m Message) Encode() ([]byte, error) {
	if m.ID == IDKeepAlive {
		return []byte{0, 0, 0, 0}, nil
	}

	var payload []byte
	switch m.ID {
	case IDChoke, IDUnchoke, IDInterested, IDNotInterested:
	case IDHave:
		payload = be32(m.Index)
	case IDBitfield:
		payload = m.Bitfield
	case IDRequest, IDCancel:
		payload = append(be32(m.Index), be32(m.Begin)...)
		payload = append(payload, be32(m.Length)...)
	case IDPiece:
		payload = append(be32(m.Index), be32(m.Begin)...)
		payload = append(payload, m.Block...)
	case IDExtended:
		if len(m.Extended) == 0 {
			return nil, fmt.Errorf("wire: extended message has no body")
		}
		payload = m.Extended
	default:
		return nil, fmt.Errorf("wire: cannot encode unknown message id %d", m.ID)
	}

	frame := make([]byte, 4+1+len(payload))
	binary.BigEndian.PutUint32(frame, uint32(1+len(payload)))
	frame[4] = m.ID
	copy(frame[5:], payload)
	return frame, nil
}

// Decode reads one frame. A zero length prefix is a keep-alive. Payload shapes
// are validated so a malformed peer cannot make the engine index out of range.
//
// Unknown message ids are returned with their id set and no payload: BEP 3
// reserves the id space for extensions, and dropping the connection over one
// we do not know yet would be wrong. Callers ignore ids they do not handle.
func Decode(r io.Reader) (Message, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Message{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		return Message{ID: IDKeepAlive}, nil
	}
	if n > MaxMessageLength {
		return Message{}, fmt.Errorf("wire: message length %d exceeds the %d byte cap", n, MaxMessageLength)
	}

	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return Message{}, err
	}
	id, payload := body[0], body[1:]
	m := Message{ID: id}

	switch id {
	case IDChoke, IDUnchoke, IDInterested, IDNotInterested:
		if len(payload) != 0 {
			return Message{}, fmt.Errorf("wire: message id %d has a %d byte payload, want 0", id, len(payload))
		}
	case IDHave:
		if len(payload) != 4 {
			return Message{}, fmt.Errorf("wire: have has a %d byte payload, want 4", len(payload))
		}
		m.Index = binary.BigEndian.Uint32(payload)
	case IDBitfield:
		m.Bitfield = append([]byte(nil), payload...)
	case IDRequest, IDCancel:
		if len(payload) != 12 {
			return Message{}, fmt.Errorf("wire: message id %d has a %d byte payload, want 12", id, len(payload))
		}
		m.Index = binary.BigEndian.Uint32(payload[0:4])
		m.Begin = binary.BigEndian.Uint32(payload[4:8])
		m.Length = binary.BigEndian.Uint32(payload[8:12])
	case IDPiece:
		if len(payload) < 8 {
			return Message{}, fmt.Errorf("wire: piece has a %d byte payload, want at least 8", len(payload))
		}
		m.Index = binary.BigEndian.Uint32(payload[0:4])
		m.Begin = binary.BigEndian.Uint32(payload[4:8])
		m.Block = append([]byte(nil), payload[8:]...)
	case IDExtended:
		if len(payload) == 0 {
			return Message{}, fmt.Errorf("wire: extended message has no body")
		}
		m.Extended = append([]byte(nil), payload...)
	default:
		// Unknown id: keep it decodable so the caller can skip it.
	}
	return m, nil
}

func be32(v uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, v)
	return buf
}
