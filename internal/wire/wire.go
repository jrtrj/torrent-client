package wire

import (
	"errors"
	"fmt"
	"io"
)

const (
	// Protocol is the pstr every BitTorrent handshake carries.
	Protocol = "BitTorrent protocol"

	// HandshakeLen is 1 (pstrlen) + 19 (pstr) + 8 (reserved) + 20 + 20.
	HandshakeLen = 68

	// BlockSize is the largest request the engine issues. The final block of
	// a piece may be shorter.
	BlockSize = 16 * 1024

	// MaxMessageLength caps an inbound frame. The largest legitimate message
	// is a 16 KiB block plus 9 bytes of framing, so this only ever rejects
	// garbage (a bitfield for a torrent with millions of pieces still fits).
	MaxMessageLength = 4 << 20
)

var (
	// ErrBadProtocol means the peer's pstrlen/pstr is not BitTorrent.
	ErrBadProtocol = errors.New("wire: peer did not speak the BitTorrent protocol")
	// ErrInfoHashMismatch means the peer answered with a different info-hash.
	ErrInfoHashMismatch = errors.New("wire: peer info-hash does not match the torrent")
)

// Handshake is the fixed 68-byte opening frame.
type Handshake struct {
	Reserved [8]byte
	InfoHash [20]byte
	PeerID   [20]byte
}

// NewHandshake builds our outbound handshake. The reserved bytes stay zero:
// DHT and the extension protocol (BEP 5/9/10) are downstream features, so
// advertising them now would invite messages we cannot answer.
func NewHandshake(infoHash, peerID [20]byte) Handshake {
	return Handshake{InfoHash: infoHash, PeerID: peerID}
}

// Encode renders the handshake as its 68 bytes.
func (h Handshake) Encode() []byte {
	buf := make([]byte, HandshakeLen)
	buf[0] = byte(len(Protocol))
	copy(buf[1:20], Protocol)
	copy(buf[20:28], h.Reserved[:])
	copy(buf[28:48], h.InfoHash[:])
	copy(buf[48:68], h.PeerID[:])
	return buf
}

// ReadHandshake reads one handshake and validates both the protocol string and
// the info-hash against the torrent we intend to talk about. A mismatch is
// connection-fatal: the peer is not in this swarm.
func ReadHandshake(r io.Reader, wantInfoHash [20]byte) (Handshake, error) {
	var h Handshake
	buf := make([]byte, HandshakeLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return h, err
	}
	if buf[0] != byte(len(Protocol)) || string(buf[1:20]) != Protocol {
		return h, ErrBadProtocol
	}
	copy(h.Reserved[:], buf[20:28])
	copy(h.InfoHash[:], buf[28:48])
	copy(h.PeerID[:], buf[48:68])
	if h.InfoHash != wantInfoHash {
		return h, ErrInfoHashMismatch
	}
	return h, nil
}

// Write sends a framed message to w.
func Write(w io.Writer, m Message) error {
	frame, err := m.Encode()
	if err != nil {
		return err
	}
	if _, err := w.Write(frame); err != nil {
		return fmt.Errorf("wire: write message id %d: %w", m.ID, err)
	}
	return nil
}
