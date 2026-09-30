// Package metadata fetches a torrent's info dictionary from the swarm with
// ut_metadata (BEP 9), which is what makes a magnet link usable: the link
// carries only an info-hash, and the metadata is reassembled from peers and
// then checked against that hash.
//
// The hash is the trust anchor. A peer is free to send any bytes it likes, so
// nothing is used until the reassembled dictionary hashes to the info-hash the
// magnet asked for.
package metadata

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"torrent-client/internal/bencode"
	"torrent-client/internal/wire"
)

const (
	// extensionHandshakeID is the extension message id reserved for the
	// extension handshake itself by BEP 10.
	extensionHandshakeID = 0

	// ourUTMetadataID is the id we advertise for ut_metadata. When sending we
	// use the id the PEER advertised, and on receive we accept any non-zero id
	// (see handleExtension), so a peer that numbers extensions differently
	// still interoperates.
	ourUTMetadataID = 1

	// metadataPieceSize is fixed by BEP 9: metadata is transferred in 16 KiB
	// pieces regardless of the torrent's own piece length.
	metadataPieceSize = 16 * 1024

	// maxMetadataSize bounds what we will believe. A real info dictionary is
	// kilobytes; anything approaching this is a peer trying to make us
	// allocate a huge buffer.
	maxMetadataSize = 4 << 20

	// maxPeersAsked bounds how many peers we interrogate at once.
	maxPeersAsked = 8
)

// ut_metadata message types.
const (
	msgRequest = 0
	msgData    = 1
	msgReject  = 2
)

// clientVersion identifies us in the extension handshake.
const clientVersion = "torrent-client 0.1"

type extensionHandshake struct {
	M            map[string]int `bencode:"m"`
	MetadataSize int64          `bencode:"metadata_size"`
	V            string         `bencode:"v"`
}

type metadataMessage struct {
	MsgType   int `bencode:"msg_type"`
	Piece     int `bencode:"piece"`
	TotalSize int `bencode:"total_size"`
}

// Fetch asks peers for the info dictionary belonging to infoHash and returns
// the first copy that hashes correctly. Peers are tried concurrently and the
// rest are abandoned as soon as one succeeds.
func Fetch(ctx context.Context, infoHash, peerID [20]byte, addrs []string) ([]byte, error) {
	if len(addrs) == 0 {
		return nil, errors.New("metadata: no peers to ask")
	}
	if len(addrs) > maxPeersAsked {
		addrs = addrs[:maxPeersAsked]
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		info []byte
		err  error
	}
	results := make(chan result, len(addrs))
	for _, addr := range addrs {
		go func(addr string) {
			info, err := fetchFromPeer(ctx, infoHash, peerID, addr)
			results <- result{info: info, err: err}
		}(addr)
	}

	var lastErr error
	for range addrs {
		r := <-results
		if r.err == nil {
			return r.info, nil
		}
		lastErr = r.err
	}
	return nil, fmt.Errorf("metadata: no peer supplied it: %w", lastErr)
}

// fetchFromPeer runs the whole exchange with one peer.
func fetchFromPeer(ctx context.Context, infoHash, peerID [20]byte, addr string) ([]byte, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// A peer that connects and then says nothing must not hold the fetch open,
	// so the whole exchange runs against one deadline.
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}
	_ = conn.SetDeadline(deadline)

	if _, err := conn.Write(wire.NewHandshake(infoHash, peerID).Encode()); err != nil {
		return nil, err
	}
	peer, err := wire.ReadHandshake(conn, infoHash)
	if err != nil {
		return nil, err
	}
	if !peer.SupportsExtensions() {
		return nil, errors.New("peer does not support the extension protocol")
	}

	if err := sendExtensionHandshake(conn); err != nil {
		return nil, err
	}
	theirID, size, err := readExtensionHandshake(conn, infoHash)
	if err != nil {
		return nil, err
	}
	if size <= 0 || size > maxMetadataSize {
		return nil, fmt.Errorf("peer advertised an implausible metadata size of %d", size)
	}

	count := int((size + metadataPieceSize - 1) / metadataPieceSize)
	buf := make([]byte, size)
	for i := 0; i < count; i++ {
		if err := requestPiece(conn, theirID, i); err != nil {
			return nil, err
		}
		data, err := readData(conn, infoHash, theirID, i)
		if err != nil {
			return nil, err
		}
		copy(buf[i*metadataPieceSize:], data)
	}

	if sha1.Sum(buf) != infoHash {
		return nil, errors.New("the metadata a peer sent does not match the info-hash")
	}
	return buf, nil
}

func sendExtensionHandshake(conn net.Conn) error {
	body, err := marshal(extensionHandshake{
		M: map[string]int{"ut_metadata": ourUTMetadataID},
		V: clientVersion,
	})
	if err != nil {
		return err
	}
	return wire.Write(conn, wire.Message{ID: wire.IDExtended, Extended: append([]byte{extensionHandshakeID}, body...)})
}

// readExtensionHandshake waits for the peer's extension handshake and reports
// the id it uses for ut_metadata and the metadata size it claims.
func readExtensionHandshake(conn net.Conn, infoHash [20]byte) (int, int64, error) {
	for {
		extID, body, err := readExtended(conn, infoHash)
		if err != nil {
			return 0, 0, err
		}
		if extID != extensionHandshakeID {
			continue
		}
		var hs extensionHandshake
		if err := bencode.Unmarshal(bytes.NewReader(body), &hs); err != nil {
			return 0, 0, fmt.Errorf("extension handshake: %w", err)
		}
		theirID, ok := hs.M["ut_metadata"]
		if !ok {
			return 0, 0, errors.New("peer does not offer ut_metadata")
		}
		if theirID == 0 {
			return 0, 0, errors.New("peer advertised ut_metadata on id 0, which is reserved")
		}
		return theirID, hs.MetadataSize, nil
	}
}

func requestPiece(conn net.Conn, theirID, piece int) error {
	body, err := marshal(metadataMessage{MsgType: msgRequest, Piece: piece})
	if err != nil {
		return err
	}
	return wire.Write(conn, wire.Message{
		ID:       wire.IDExtended,
		Extended: append([]byte{byte(theirID)}, body...),
	})
}

func marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := bencode.Marshal(&buf, v); err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	return buf.Bytes(), nil
}

// readData waits for the data message for piece, skipping anything else.
func readData(conn net.Conn, infoHash [20]byte, theirID, piece int) ([]byte, error) {
	for {
		extID, body, err := readExtended(conn, infoHash)
		if err != nil {
			return nil, err
		}
		if extID == extensionHandshakeID {
			continue
		}
		dict, data, err := splitBencodePrefix(body)
		if err != nil {
			return nil, fmt.Errorf("ut_metadata message: %w", err)
		}
		var msg metadataMessage
		if err := bencode.Unmarshal(bytes.NewReader(dict), &msg); err != nil {
			return nil, fmt.Errorf("ut_metadata message: %w", err)
		}
		switch msg.MsgType {
		case msgData:
			if msg.Piece != piece {
				continue
			}
			return data, nil
		case msgReject:
			return nil, fmt.Errorf("peer rejected the request for metadata piece %d", piece)
		default:
			// A request aimed at us; we have no metadata to serve.
			continue
		}
	}
}

// readExtended returns the extension message id and body of the next extended
// message, ignoring ordinary protocol messages that arrive in between.
func readExtended(conn net.Conn, infoHash [20]byte) (byte, []byte, error) {
	for {
		m, err := wire.Decode(conn)
		if err != nil {
			return 0, nil, err
		}
		switch m.ID {
		case wire.IDKeepAlive, wire.IDBitfield, wire.IDHave,
			wire.IDChoke, wire.IDUnchoke, wire.IDInterested, wire.IDNotInterested:
			continue
		case wire.IDExtended:
			return m.Extended[0], m.Extended[1:], nil
		default:
			// Anything else is not ours to act on here.
			continue
		}
	}
}

// splitBencodePrefix returns the first complete bencoded value in b and the
// bytes after it. A ut_metadata data message is a bencoded dictionary with the
// raw metadata appended, so the split has to be exact — decoding the whole
// body as bencode would fail on the trailing bytes.
func splitBencodePrefix(b []byte) ([]byte, []byte, error) {
	if len(b) == 0 || b[0] != 'd' {
		return nil, nil, errors.New("extended body does not start with a dictionary")
	}
	depth := 0
	for i := 0; i < len(b); {
		switch c := b[i]; {
		case c == 'd' || c == 'l':
			depth++
			i++
		case c == 'e':
			depth--
			i++
			if depth == 0 {
				return b[:i], b[i:], nil
			}
		case c == 'i':
			j := bytes.IndexByte(b[i:], 'e')
			if j < 0 {
				return nil, nil, errors.New("unterminated integer")
			}
			i += j + 1
		case c >= '0' && c <= '9':
			j := bytes.IndexByte(b[i:], ':')
			if j < 0 {
				return nil, nil, errors.New("unterminated string length")
			}
			n, err := strconv.Atoi(string(b[i : i+j]))
			if err != nil {
				return nil, nil, fmt.Errorf("string length: %w", err)
			}
			i += j + 1 + n
			if i > len(b) {
				return nil, nil, errors.New("string runs past the end of the message")
			}
		default:
			return nil, nil, fmt.Errorf("unexpected byte %q", c)
		}
	}
	return nil, nil, errors.New("no complete bencoded value")
}
