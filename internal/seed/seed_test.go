package seed

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"torrent-client/internal/bencode"
	"torrent-client/internal/metainfo"
	"torrent-client/internal/storage"
	"torrent-client/internal/wire"
)

// These tests drive the real server over real loopback TCP with a scripted
// peer, so what they prove is wire behaviour: what is served, what is refused,
// and who gets unchoked.

const testPieceLength = 64 * 1024

// testSource is a Source whose held set can change while a peer is connected,
// so a test can reproduce a piece verifying mid-download. It counts reads so a
// test can prove a refused request never reached the disk.
type testSource struct {
	store  *storage.Storage
	pieces int

	mu    sync.Mutex
	held  map[int]bool
	reads int
}

func newTestSource(store *storage.Storage, meta *metainfo.MetaInfo, held ...int) *testSource {
	s := &testSource{store: store, pieces: meta.PieceCount(), held: make(map[int]bool)}
	for _, p := range held {
		s.held[p] = true
	}
	return s
}

func (s *testSource) HaveBitfield() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	bf := wire.NewBitfield(s.pieces)
	for i := range s.held {
		wire.BitfieldSet(bf, i)
	}
	return bf
}

func (s *testSource) hold(index int) {
	s.mu.Lock()
	s.held[index] = true
	s.mu.Unlock()
}

func (s *testSource) holdAll(n int) {
	for i := 0; i < n; i++ {
		s.hold(i)
	}
}

func (s *testSource) Have(index int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.held[index]
}

func (s *testSource) ReadBlock(index int, begin, length uint32) ([]byte, error) {
	s.mu.Lock()
	s.reads++
	s.mu.Unlock()
	return s.store.ReadBlock(index, begin, length)
}

func (s *testSource) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

type peer struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func testPeerID() [20]byte {
	var id [20]byte
	copy(id[:], "-SEED0001-testpeer")
	return id
}

// newTestServer starts a server over 127.0.0.1 on an OS-assigned port and
// stops it at test cleanup.
func newTestServer(t *testing.T, meta *metainfo.MetaInfo, src Source) *Server {
	t.Helper()
	srv, err := New(Config{
		Meta:   meta,
		Source: src,
		PeerID: testPeerID(),
		Listen: "127.0.0.1:0",
		Log:    t.Logf,
	})
	if err != nil {
		t.Fatalf("seed.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		srv.Close()
	})
	go func() { _ = srv.Serve(ctx) }()
	return srv
}

// dialSeed connects, exchanges handshakes for meta's info-hash, and returns
// the scripted peer.
func dialSeed(t *testing.T, srv *Server, meta *metainfo.MetaInfo) *peer {
	t.Helper()
	return dial(t, srv.Addr().String(), meta.InfoHash, meta.InfoHash)
}

// dial connects and exchanges handshakes. send is the info-hash we claim;
// expect is the one we require from the server's handshake, so a test can send
// a foreign info-hash and still read the server's own reply.
func dial(t *testing.T, addr string, send, expect [20]byte) *peer {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	var id [20]byte
	copy(id[:], "-TESTPEER00000000001")
	if _, err := conn.Write(wire.NewHandshake(send, id).Encode()); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
	if _, err := wire.ReadHandshake(conn, expect); err != nil {
		t.Fatalf("read handshake: %v", err)
	}
	return &peer{t: t, conn: conn, r: bufio.NewReader(conn)}
}

func (p *peer) write(m wire.Message) {
	p.t.Helper()
	if err := p.conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		p.t.Fatalf("set write deadline: %v", err)
	}
	if err := wire.Write(p.conn, m); err != nil {
		p.t.Fatalf("write %d: %v", m.ID, err)
	}
}

func (p *peer) read() (wire.Message, error) {
	if err := p.conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return wire.Message{}, err
	}
	return wire.Decode(p.r)
}

// expect reads until a message with the given id, skipping the haves the
// advertiser may interleave. Anything else fails the test.
func (p *peer) expect(id byte) wire.Message {
	p.t.Helper()
	for {
		m, err := p.read()
		if err != nil {
			p.t.Fatalf("read while waiting for message %d: %v", id, err)
		}
		if m.ID == id {
			return m
		}
		if m.ID == wire.IDHave {
			continue
		}
		p.t.Fatalf("got message %d, want %d", m.ID, id)
	}
}

// expectClosed requires the server to drop the connection rather than answer.
func (p *peer) expectClosed(why string) {
	p.t.Helper()
	if err := p.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		p.t.Fatalf("set read deadline: %v", err)
	}
	if _, err := wire.Decode(p.r); err == nil {
		p.t.Fatalf("server kept the connection after %s", why)
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		p.t.Fatalf("server left the connection open after %s (no reply, no close)", why)
	}
}

func (p *peer) close() { p.conn.Close() }

func testPayload(t *testing.T, n int) []byte {
	t.Helper()
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return buf
}

// testTorrent builds a single-file metainfo over data by taking the real
// bencode round trip, so the info-hash and piece hashes are genuine.
func testTorrent(t *testing.T, data []byte, pieceLength int64) *metainfo.MetaInfo {
	t.Helper()
	var pieces []byte
	for off := 0; off < len(data); off += int(pieceLength) {
		end := off + int(pieceLength)
		if end > len(data) {
			end = len(data)
		}
		sum := sha1.Sum(data[off:end])
		pieces = append(pieces, sum[:]...)
	}
	infoBytes := mustMarshal(t, struct {
		Length      int64  `bencode:"length"`
		Name        string `bencode:"name"`
		PieceLength int64  `bencode:"piece length"`
		Pieces      []byte `bencode:"pieces"`
	}{int64(len(data)), "payload.bin", pieceLength, pieces})

	raw := mustMarshal(t, struct {
		Announce string             `bencode:"announce"`
		Info     bencode.RawMessage `bencode:"info"`
	}{"http://127.0.0.1:1/announce", infoBytes})

	meta, err := metainfo.Parse(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("parse test torrent: %v", err)
	}
	return meta
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := bencode.Marshal(&buf, v); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return buf.Bytes()
}

func testStore(t *testing.T, meta *metainfo.MetaInfo, data []byte) *storage.Storage {
	t.Helper()
	st, err := storage.Open(filepath.Join(t.TempDir(), "content.bin"), meta.Info.PieceLength, meta.TotalLength())
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	for i := 0; i < meta.PieceCount(); i++ {
		off := int64(i) * meta.Info.PieceLength
		if err := st.WritePiece(i, data[off:off+meta.PieceSize(i)]); err != nil {
			t.Fatalf("WritePiece(%d): %v", i, err)
		}
	}
	return st
}

// series produces deterministic non-random data for tests that only care about
// byte correctness.
func series(n int) []byte {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte(i * 7)
	}
	return buf
}

// The core proof: a peer handshakes, gets interested, requests every block, and
// gets byte-correct data back while the server's upload counters move.
func TestServesHeldPiecesByteCorrect(t *testing.T) {
	data := testPayload(t, 3*testPieceLength)
	meta := testTorrent(t, data, testPieceLength)
	store := testStore(t, meta, data)
	src := newTestSource(store, meta)
	src.holdAll(meta.PieceCount())
	srv := newTestServer(t, meta, src)

	p := dialSeed(t, srv, meta)
	defer p.close()

	bf := p.expect(wire.IDBitfield)
	if got := wire.BitfieldCount(bf.Bitfield, meta.PieceCount()); got != meta.PieceCount() {
		t.Fatalf("bitfield advertises %d of %d pieces", got, meta.PieceCount())
	}
	p.write(wire.Message{ID: wire.IDInterested})
	p.expect(wire.IDUnchoke)

	got := make([]byte, len(data))
	blocks := 0
	for i := 0; i < meta.PieceCount(); i++ {
		size := meta.PieceSize(i)
		for begin := int64(0); begin < size; begin += wire.BlockSize {
			length := min(int64(wire.BlockSize), size-begin)
			p.write(wire.Message{ID: wire.IDRequest, Index: uint32(i), Begin: uint32(begin), Length: uint32(length)})
			m := p.expect(wire.IDPiece)
			if int(m.Index) != i || m.Begin != uint32(begin) {
				t.Fatalf("piece reply is %d+%d, want %d+%d", m.Index, m.Begin, i, begin)
			}
			if int64(len(m.Block)) != length {
				t.Fatalf("block %d+%d is %d bytes, want %d", i, begin, len(m.Block), length)
			}
			copy(got[int64(i)*testPieceLength+begin:], m.Block)
			blocks++
		}
	}

	if !bytes.Equal(got, data) {
		t.Fatalf("served data differs from the source")
	}
	if served := srv.BlocksServed(); served != int64(blocks) {
		t.Fatalf("blocks served = %d, want %d", served, blocks)
	}
	if up := srv.Uploaded(); up != int64(len(data)) {
		t.Fatalf("uploaded = %d bytes, want %d", up, len(data))
	}
}

// The swarm gate: a peer whose handshake does not name the torrent is dropped,
// whether the info-hash or the protocol magic is wrong.
func TestDropsForeignHandshake(t *testing.T) {
	data := series(2 * testPieceLength)
	meta := testTorrent(t, data, testPieceLength)
	store := testStore(t, meta, data)
	src := newTestSource(store, meta)
	src.holdAll(meta.PieceCount())
	srv := newTestServer(t, meta, src)

	t.Run("wrong info-hash", func(t *testing.T) {
		var wrong [20]byte
		wrong[0] = 0xff
		p := dial(t, srv.Addr().String(), wrong, meta.InfoHash)
		defer p.close()
		// The server writes its handshake before it validates ours, so read it
		// and then require the connection to be dropped.
		if err := p.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := wire.Decode(p.r); err == nil {
			t.Fatal("server answered a foreign info-hash")
		} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
			t.Fatal("server left a foreign info-hash connection open")
		}
	})

	t.Run("wrong protocol string", func(t *testing.T) {
		conn, err := net.Dial("tcp", srv.Addr().String())
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		bad := wire.NewHandshake(meta.InfoHash, testPeerID()).Encode()
		bad[1] = 'X' // corrupt the pstr
		if _, err := conn.Write(bad); err != nil {
			t.Fatalf("write handshake: %v", err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		// Drain whatever the server wrote, then require a close.
		buf := make([]byte, 68)
		for {
			_, err := conn.Read(buf)
			if err == nil {
				continue
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatal("server left a bad-protocol connection open")
			}
			return
		}
	})
}

// Every bad request is rejected without a panic and without a disk read:
// out-of-range pieces and malformed lengths drop the connection, while a
// request for a piece we do not hold yet is ignored and the connection keeps
// serving.
func TestRefusesOutOfBoundsRequests(t *testing.T) {
	data := series(2 * testPieceLength)
	meta := testTorrent(t, data, testPieceLength)
	store := testStore(t, meta, data)
	src := newTestSource(store, meta, 0)
	srv := newTestServer(t, meta, src)

	drops := []struct {
		name string
		msg  wire.Message
	}{
		{"piece index past the end", wire.Message{ID: wire.IDRequest, Index: uint32(meta.PieceCount() + 1), Begin: 0, Length: 16}},
		{"piece index far past the end", wire.Message{ID: wire.IDRequest, Index: 1 << 20, Begin: 0, Length: 16}},
		{"length zero", wire.Message{ID: wire.IDRequest, Index: 0, Begin: 0, Length: 0}},
		{"length over the cap", wire.Message{ID: wire.IDRequest, Index: 0, Begin: 0, Length: MaxRequestLength + 1}},
		{"span overruns the piece", wire.Message{ID: wire.IDRequest, Index: 0, Begin: uint32(meta.PieceSize(0) - 8), Length: 64}},
	}
	for _, tc := range drops {
		t.Run(tc.name, func(t *testing.T) {
			before := src.readCount()
			p := dialSeed(t, srv, meta)
			defer p.close()
			p.expect(wire.IDBitfield)
			p.write(wire.Message{ID: wire.IDInterested})
			p.expect(wire.IDUnchoke)

			p.write(tc.msg)
			p.expectClosed(tc.name)
			if got := src.readCount(); got != before {
				t.Fatalf("a refused request reached the disk (%d reads, want %d)", got, before)
			}
		})
	}

	t.Run("piece not held yet is ignored", func(t *testing.T) {
		before := src.readCount()
		p := dialSeed(t, srv, meta)
		defer p.close()
		p.expect(wire.IDBitfield)
		p.write(wire.Message{ID: wire.IDInterested})
		p.expect(wire.IDUnchoke)

		// Piece 1 is not held: the request is ignored, not fatal.
		p.write(wire.Message{ID: wire.IDRequest, Index: 1, Begin: 0, Length: 16})
		if got := src.readCount(); got != before {
			t.Fatalf("a request for a piece we do not hold reached the disk")
		}

		// The connection is still good: a request for a held piece is served.
		p.write(wire.Message{ID: wire.IDRequest, Index: 0, Begin: 0, Length: 16})
		m := p.expect(wire.IDPiece)
		if want := data[:16]; !bytes.Equal(m.Block, want) {
			t.Fatalf("served %x, want %x", m.Block, want)
		}
	})
}

// The choke policy: no more than uploadSlots interested peers are unchoked at
// once.
func TestUploadSlotsBoundConcurrentPeers(t *testing.T) {
	data := series(testPieceLength)
	meta := testTorrent(t, data, testPieceLength)
	store := testStore(t, meta, data)
	src := newTestSource(store, meta)
	src.holdAll(meta.PieceCount())
	srv := newTestServer(t, meta, src)

	peers := make([]*peer, 0, uploadSlots+2)
	for i := 0; i < uploadSlots+2; i++ {
		p := dialSeed(t, srv, meta)
		defer p.close()
		p.expect(wire.IDBitfield)
		p.write(wire.Message{ID: wire.IDInterested})
		peers = append(peers, p)
	}

	unchoked := 0
	for _, p := range peers {
		if err := p.conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		m, err := wire.Decode(p.r)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue // stayed choked, which is the bound working
			}
			t.Fatalf("read while counting unchokes: %v", err)
		}
		if m.ID != wire.IDUnchoke {
			t.Fatalf("got message %d, want unchoke", m.ID)
		}
		unchoked++
	}
	if unchoked != uploadSlots {
		t.Fatalf("unchoked %d of %d interested peers, want exactly %d", unchoked, len(peers), uploadSlots)
	}
}

// A peer that connects before a piece exists is told about it once it verifies,
// and can then fetch it. This is what makes serving during the download useful.
func TestAdvertisesPiecesVerifiedLater(t *testing.T) {
	data := series(2 * testPieceLength)
	meta := testTorrent(t, data, testPieceLength)
	store := testStore(t, meta, data)
	src := newTestSource(store, meta) // nothing held yet
	srv := newTestServer(t, meta, src)

	p := dialSeed(t, srv, meta)
	defer p.close()

	bf := p.expect(wire.IDBitfield)
	if got := wire.BitfieldCount(bf.Bitfield, meta.PieceCount()); got != 0 {
		t.Fatalf("peer was told about %d pieces that are not held", got)
	}

	// A piece verifies mid-connection.
	src.hold(0)
	src.hold(1)

	seen := make(map[int]bool)
	// Generous: the advertiser ticks at a fixed rate and a loaded box can delay
	// it. What matters is that the have arrives at all, not that it lands inside
	// a scheduler-dependent window - asserting on those is a pain in the ass.
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < 2 && time.Now().Before(deadline) {
		m, err := p.read()
		if err != nil {
			t.Fatalf("read while waiting for haves: %v", err)
		}
		if m.ID == wire.IDHave {
			seen[int(m.Index)] = true
		}
	}
	if !seen[0] || !seen[1] {
		t.Fatalf("saw haves %v, want 0 and 1", seen)
	}

	// Now the piece is servable: interest, unchoke, fetch.
	p.write(wire.Message{ID: wire.IDInterested})
	p.expect(wire.IDUnchoke)
	p.write(wire.Message{ID: wire.IDRequest, Index: 0, Begin: 0, Length: 32})
	m := p.expect(wire.IDPiece)
	if want := data[:32]; !bytes.Equal(m.Block, want) {
		t.Fatalf("served %x, want %x", m.Block, want)
	}
}

// Serve and Close leave nothing running: after Close, an open peer connection
// is dropped and the accept loop has stopped.
func TestShutdownClosesConnections(t *testing.T) {
	data := series(testPieceLength)
	meta := testTorrent(t, data, testPieceLength)
	store := testStore(t, meta, data)
	src := newTestSource(store, meta, 0)
	srv := newTestServer(t, meta, src)

	p := dialSeed(t, srv, meta)
	defer p.close()
	p.expect(wire.IDBitfield)

	if err := srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := p.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(p.r); err != nil && !errors.Is(err, io.EOF) {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			t.Fatal("connection survived Close")
		}
	}
}
