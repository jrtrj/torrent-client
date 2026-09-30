package metadata

import (
	"bytes"
	"context"
	"crypto/sha1"
	"net"
	"testing"
	"time"

	"torrent-client/internal/bencode"
	"torrent-client/internal/metainfo"
	"torrent-client/internal/wire"
)

// sampleInfo returns a real info dictionary together with its info-hash.
//
// The piece list is deliberately large: 900 pieces is 18 KB of metadata, which
// crosses the 16 KiB metadata-piece boundary, so the transfer genuinely spans
// more than one ut_metadata piece instead of passing on a single request.
func sampleInfo(t *testing.T) ([]byte, [20]byte) {
	t.Helper()
	var buf bytes.Buffer
	err := bencode.Marshal(&buf, struct {
		Length      int64  `bencode:"length"`
		Name        string `bencode:"name"`
		PieceLength int64  `bencode:"piece length"`
		Pieces      string `bencode:"pieces"`
	}{
		Length:      900 * 16384,
		Name:        "big.bin",
		PieceLength: 16384,
		Pieces:      string(bytes.Repeat([]byte{0x5a}, 20*900)),
	})
	if err != nil {
		t.Fatalf("marshal the sample info: %v", err)
	}
	raw := buf.Bytes()
	return raw, sha1.Sum(raw)
}

// fakePeer is a scripted peer that speaks the extension protocol and serves an
// info dictionary over ut_metadata.
type fakePeer struct {
	infoHash [20]byte
	meta     []byte

	// utMetadataID is the id this peer advertises. It is deliberately not the
	// id we advertise, so a client that ignores the peer's numbering fails.
	utMetadataID int

	noExtensions bool // replies without the BEP 10 bit set
	noUTMetadata bool // omits ut_metadata from the m dictionary
	corrupt      bool // serves metadata that will not hash correctly

	seenRequestIDs chan int
	ln             net.Listener
}

// Peer options. They must be applied before the accept loop starts: the flags
// are read by the connection goroutines and never written again, so setting
// them up front is what keeps these tests race-free.
func corruptingTheMetadata(p *fakePeer) { p.corrupt = true }
func withoutExtensions(p *fakePeer)     { p.noExtensions = true }
func withoutUTMetadata(p *fakePeer)     { p.noUTMetadata = true }

func newFakePeer(t *testing.T, infoHash [20]byte, meta []byte, opts ...func(*fakePeer)) *fakePeer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &fakePeer{
		infoHash:       infoHash,
		meta:           meta,
		utMetadataID:   3,
		seenRequestIDs: make(chan int, 64),
		ln:             ln,
	}
	for _, opt := range opts {
		opt(p)
	}
	go p.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return p
}

func (p *fakePeer) addr() string { return p.ln.Addr().String() }

func (p *fakePeer) serve() {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			p.handle(conn)
		}()
	}
}

func (p *fakePeer) handle(conn net.Conn) {
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))

	hs := wire.NewHandshake(p.infoHash, [20]byte{})
	if p.noExtensions {
		hs.Reserved = [8]byte{}
	}
	if _, err := conn.Write(hs.Encode()); err != nil {
		return
	}
	if _, err := wire.ReadHandshake(conn, p.infoHash); err != nil {
		return
	}

	for {
		m, err := wire.Decode(conn)
		if err != nil {
			return
		}
		if m.ID != wire.IDExtended {
			continue
		}
		extID, body := m.Extended[0], m.Extended[1:]
		if extID == extensionHandshakeID {
			if err := p.sendHandshake(conn); err != nil {
				return
			}
			continue
		}
		select {
		case p.seenRequestIDs <- int(extID):
		default:
		}
		if err := p.reply(conn, body); err != nil {
			return
		}
	}
}

func (p *fakePeer) sendHandshake(conn net.Conn) error {
	hs := extensionHandshake{V: "fake", MetadataSize: int64(len(p.meta))}
	if !p.noUTMetadata {
		hs.M = map[string]int{"ut_metadata": p.utMetadataID}
	}
	body, err := marshal(hs)
	if err != nil {
		return err
	}
	return wire.Write(conn, wire.Message{
		ID:       wire.IDExtended,
		Extended: append([]byte{extensionHandshakeID}, body...),
	})
}

func (p *fakePeer) reply(conn net.Conn, body []byte) error {
	var msg metadataMessage
	if err := bencode.Unmarshal(bytes.NewReader(body), &msg); err != nil {
		return err
	}
	if msg.MsgType != msgRequest {
		return nil
	}

	start := msg.Piece * metadataPieceSize
	if start < 0 || start >= len(p.meta) {
		out, err := marshal(metadataMessage{MsgType: msgReject, Piece: msg.Piece})
		if err != nil {
			return err
		}
		return wire.Write(conn, wire.Message{
			ID:       wire.IDExtended,
			Extended: append([]byte{byte(ourUTMetadataID)}, out...),
		})
	}
	end := start + metadataPieceSize
	if end > len(p.meta) {
		end = len(p.meta)
	}
	piece := bytes.Clone(p.meta[start:end])
	if p.corrupt {
		piece[0] ^= 0xff
	}

	out, err := marshal(metadataMessage{MsgType: msgData, Piece: msg.Piece, TotalSize: len(p.meta)})
	if err != nil {
		return err
	}
	// A data message is the dictionary immediately followed by the raw piece.
	payload := append(append([]byte{byte(ourUTMetadataID)}, out...), piece...)
	return wire.Write(conn, wire.Message{ID: wire.IDExtended, Extended: payload})
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestFetchReassemblesAPieceFromThePeer(t *testing.T) {
	meta, hash := sampleInfo(t)
	if len(meta) <= metadataPieceSize {
		t.Fatalf("the fixture metadata is %d bytes; it must exceed %d to exercise a multi-piece transfer",
			len(meta), metadataPieceSize)
	}
	peer := newFakePeer(t, hash, meta)

	got, err := Fetch(testContext(t), hash, [20]byte{9}, []string{peer.addr()})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !bytes.Equal(got, meta) {
		t.Fatalf("fetched %d bytes, want the %d bytes the peer served", len(got), len(meta))
	}
	if sha1.Sum(got) != hash {
		t.Fatal("the fetched metadata does not hash to the requested info-hash")
	}

	// The bytes must be usable as a torrent: the whole point is to hand them
	// to the parser and download from them.
	m, err := metainfo.ParseInfoBytes(got, []string{"http://t/announce"})
	if err != nil {
		t.Fatalf("the fetched metadata is not a parsable info dictionary: %v", err)
	}
	if m.Info.Name != "big.bin" || m.InfoHash != hash {
		t.Fatalf("parsed name/hash = %q/%x, want big.bin/%x", m.Info.Name, m.InfoHash, hash)
	}

	// Requests must go out on the id the PEER advertised, not ours.
	select {
	case id := <-peer.seenRequestIDs:
		if id != peer.utMetadataID {
			t.Fatalf("request used extension id %d; the peer advertised ut_metadata on %d",
				id, peer.utMetadataID)
		}
	default:
		t.Fatal("the peer saw no request")
	}
}

func TestFetchRejectsMetadataThatDoesNotMatchTheHash(t *testing.T) {
	meta, hash := sampleInfo(t)
	peer := newFakePeer(t, hash, meta, corruptingTheMetadata)

	if got, err := Fetch(testContext(t), hash, [20]byte{9}, []string{peer.addr()}); err == nil {
		t.Fatalf("Fetch accepted %d bytes of corrupt metadata", len(got))
	}
}

func TestFetchSkipsPeersThatCannotHelp(t *testing.T) {
	meta, hash := sampleInfo(t)

	silent := newFakePeer(t, hash, meta, withoutExtensions)
	noUT := newFakePeer(t, hash, meta, withoutUTMetadata)
	good := newFakePeer(t, hash, meta)

	got, err := Fetch(testContext(t), hash, [20]byte{9},
		[]string{silent.addr(), noUT.addr(), good.addr()})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !bytes.Equal(got, meta) {
		t.Fatal("Fetch returned metadata that differs from what the good peer served")
	}
}

func TestFetchFailsWhenNoPeerHasIt(t *testing.T) {
	meta, hash := sampleInfo(t)
	other := hash
	other[0] ^= 0xff

	// Every peer's served metadata hashes to something else, so none of them
	// can be trusted no matter how well the transfer goes.
	a := newFakePeer(t, hash, meta, corruptingTheMetadata)
	b := newFakePeer(t, hash, meta, withoutExtensions)

	if got, err := Fetch(testContext(t), hash, [20]byte{9}, []string{a.addr(), b.addr()}); err == nil {
		t.Fatalf("Fetch accepted %d bytes when no peer could supply the real metadata", len(got))
	}
}

func TestFetchRefusesWithNoPeers(t *testing.T) {
	_, hash := sampleInfo(t)
	if _, err := Fetch(testContext(t), hash, [20]byte{9}, nil); err == nil {
		t.Fatal("Fetch accepted an empty peer list")
	}
}

func TestSplitBencodePrefixSeparatesTheDictionaryFromThePayload(t *testing.T) {
	dict := []byte("d8:msg_typei1e5:piecei0ee")
	payload := []byte("RAW")
	gotDict, gotRest, err := splitBencodePrefix(append(bytes.Clone(dict), payload...))
	if err != nil {
		t.Fatalf("splitBencodePrefix: %v", err)
	}
	if !bytes.Equal(gotDict, dict) {
		t.Errorf("dictionary = %q, want %q", gotDict, dict)
	}
	if !bytes.Equal(gotRest, payload) {
		t.Errorf("remainder = %q, want %q", gotRest, payload)
	}
}

// Binary that happens to look like bencode must not be mistaken for the end of
// the dictionary: the split has to count, not search.
func TestSplitBencodePrefixIsNotFooledByBinaryInThePayload(t *testing.T) {
	dict := []byte("d5:piecei0ee")
	payload := []byte("eeee0:9999999999:")
	gotDict, gotRest, err := splitBencodePrefix(append(bytes.Clone(dict), payload...))
	if err != nil {
		t.Fatalf("splitBencodePrefix: %v", err)
	}
	if !bytes.Equal(gotDict, dict) {
		t.Errorf("dictionary = %q, want %q", gotDict, dict)
	}
	if !bytes.Equal(gotRest, payload) {
		t.Errorf("remainder = %q, want %q", gotRest, payload)
	}
}

func TestSplitBencodePrefixHandlesNestedDictionaries(t *testing.T) {
	dict := []byte("d1:ad1:bd1:ci1eeee")
	_, rest, err := splitBencodePrefix(append(bytes.Clone(dict), []byte("TAIL")...))
	if err != nil {
		t.Fatalf("splitBencodePrefix: %v", err)
	}
	if string(rest) != "TAIL" {
		t.Fatalf("remainder = %q, want TAIL", rest)
	}
}

func TestSplitBencodePrefixRejectsNonDictionaryBodies(t *testing.T) {
	for _, in := range []string{"", "i5e", "3:abc"} {
		if _, _, err := splitBencodePrefix([]byte(in)); err == nil {
			t.Fatalf("splitBencodePrefix accepted %q", in)
		}
	}
}
