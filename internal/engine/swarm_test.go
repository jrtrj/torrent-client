package engine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"torrent-client/internal/bencode"
	"torrent-client/internal/metainfo"
	"torrent-client/internal/tracker"
	"torrent-client/internal/wire"
)

// These tests drive the real engine over real loopback TCP against scripted
// peers, so what they prove is the wire behaviour of the scheduler: who gets
// asked for what, who is dropped, and whether work is ever duplicated.

// fakeTracker hands out a fixed peer set so a test decides the swarm exactly.
type fakeTracker struct{ peers []tracker.Peer }

func (f *fakeTracker) Announce(context.Context, tracker.AnnounceRequest) (tracker.AnnounceResponse, error) {
	return tracker.AnnounceResponse{Interval: time.Hour, Peers: f.peers}, nil
}

// req is one block request or response, in the terms the assertions use.
type req struct {
	piece  int
	begin  uint32
	length uint32
}

func (r req) key() [2]uint32 { return [2]uint32{uint32(r.piece), r.begin} }

type seederConfig struct {
	// advertise is the bitfield the seeder opens with.
	advertise []int
	// corrupt lists pieces whose every block is served with a flipped byte.
	corrupt []int
	// stall lists pieces whose requests are accepted and never answered.
	stall []int
	// delay is slept before every block response, widening the serving window.
	delay time.Duration
	// chokeAfter makes the seeder choke the client after it has answered this
	// many blocks; zero means it never chokes.
	chokeAfter int
}

type fakeSeeder struct {
	t    *testing.T
	name string
	meta *metainfo.MetaInfo
	data []byte
	cfg  seederConfig

	ln  net.Listener
	wg  sync.WaitGroup
	end chan struct{}

	// mu guards what the assertions read back: the requests it saw, the blocks
	// it served, the haves the client broadcast, any client protocol violation,
	// and its live connections.
	mu        sync.Mutex
	requests  []req
	served    []req
	haves     []int
	violation []string
	conns     []*seederConn
}

type seederConn struct {
	conn net.Conn
	r    *bufio.Reader

	wmu sync.Mutex
	// amu guards advertise: the test announces new pieces from its own
	// goroutine while the serving goroutine reads the set.
	amu       sync.Mutex
	advertise map[int]bool
}

func (sc *seederConn) advertises(piece int) bool {
	sc.amu.Lock()
	defer sc.amu.Unlock()
	return sc.advertise[piece]
}

func (sc *seederConn) advertisePiece(piece int) {
	sc.amu.Lock()
	sc.advertise[piece] = true
	sc.amu.Unlock()
}

func (sc *seederConn) write(m wire.Message) error {
	sc.wmu.Lock()
	defer sc.wmu.Unlock()
	return wire.Write(sc.conn, m)
}

func newFakeSeeder(t *testing.T, name string, meta *metainfo.MetaInfo, data []byte, cfg seederConfig) *fakeSeeder {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakeSeeder{t: t, name: name, meta: meta, data: data, cfg: cfg, ln: ln, end: make(chan struct{})}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.serveConn(conn)
			}()
		}
	}()
	t.Cleanup(s.close)
	return s
}

func (s *fakeSeeder) close() {
	close(s.end)
	s.ln.Close()
	s.wg.Wait()
}

func (s *fakeSeeder) peer() tracker.Peer {
	addr := s.ln.Addr().(*net.TCPAddr)
	return tracker.Peer{IP: addr.IP, Port: uint16(addr.Port)}
}

// servedCount is how many blocks the seeder has answered.
func (s *fakeSeeder) servedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.served)
}

func (s *fakeSeeder) requestCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

func (s *fakeSeeder) servedBlocks() []req {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]req(nil), s.served...)
}

func (s *fakeSeeder) violations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.violation...)
}

// havesSeen is the piece indices the client announced to this seeder.
func (s *fakeSeeder) havesSeen() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.haves...)
}

// sendHave announces a piece to every connection, standing in for a peer that
// gains data after we first met it.
func (s *fakeSeeder) sendHave(index int) {
	s.mu.Lock()
	conns := append([]*seederConn(nil), s.conns...)
	s.mu.Unlock()
	for _, sc := range conns {
		sc.advertisePiece(index)
		_ = sc.write(wire.Message{ID: wire.IDHave, Index: uint32(index)})
	}
}

func (s *fakeSeeder) serveConn(conn net.Conn) {
	defer conn.Close()
	sc := &seederConn{
		conn:      conn,
		r:         bufio.NewReader(conn),
		advertise: make(map[int]bool),
	}
	for _, i := range s.cfg.advertise {
		sc.advertisePiece(i)
	}

	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return
	}
	var peerID [20]byte
	copy(peerID[:], "-FC0001-")
	if _, err := conn.Write(wire.NewHandshake(s.meta.InfoHash, peerID).Encode()); err != nil {
		return
	}
	if _, err := wire.ReadHandshake(conn, s.meta.InfoHash); err != nil {
		return
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return
	}

	bf := wire.NewBitfield(s.meta.PieceCount())
	for i := range sc.advertise {
		wire.BitfieldSet(bf, i)
	}
	if err := sc.write(wire.Message{ID: wire.IDBitfield, Bitfield: bf}); err != nil {
		return
	}
	s.mu.Lock()
	s.conns = append(s.conns, sc)
	s.mu.Unlock()

	interested, choked, answered := false, false, 0
	for {
		m, err := wire.Decode(sc.r)
		if err != nil {
			return
		}
		switch m.ID {
		case wire.IDHave:
			s.mu.Lock()
			s.haves = append(s.haves, int(m.Index))
			s.mu.Unlock()
		case wire.IDInterested:
			if !interested && !choked {
				if err := sc.write(wire.Message{ID: wire.IDUnchoke}); err != nil {
					return
				}
				interested = true
			}
		case wire.IDRequest:
			b := req{piece: int(m.Index), begin: m.Begin, length: m.Length}
			s.mu.Lock()
			s.requests = append(s.requests, b)
			s.mu.Unlock()
			if choked || contains(s.cfg.stall, b.piece) {
				continue
			}
			if err := s.serveBlock(sc, b); err != nil {
				return
			}
			answered++
			if s.cfg.chokeAfter > 0 && answered >= s.cfg.chokeAfter {
				if err := sc.write(wire.Message{ID: wire.IDChoke}); err != nil {
					return
				}
				choked = true
			}
		}
	}
}

// serveBlock validates the request the way a real seeder must, serves it, and
// records anything the client got wrong.
func (s *fakeSeeder) serveBlock(sc *seederConn, b req) error {
	if b.piece < 0 || b.piece >= s.meta.PieceCount() {
		s.mu.Lock()
		s.violation = append(s.violation, fmt.Sprintf("request for out-of-range piece %d", b.piece))
		s.mu.Unlock()
		return fmt.Errorf("bad piece")
	}
	if !sc.advertises(b.piece) {
		s.mu.Lock()
		s.violation = append(s.violation, fmt.Sprintf("request for piece %d the peer never advertised", b.piece))
		s.mu.Unlock()
		return fmt.Errorf("unadvertised piece")
	}
	if int64(b.begin)+int64(b.length) > s.meta.PieceSize(b.piece) || b.length == 0 {
		s.mu.Lock()
		s.violation = append(s.violation, fmt.Sprintf("request %d+%d overruns piece %d of %d bytes",
			b.begin, b.length, b.piece, s.meta.PieceSize(b.piece)))
		s.mu.Unlock()
		return fmt.Errorf("overrun")
	}

	off := int64(b.piece)*s.meta.Info.PieceLength + int64(b.begin)
	block := append([]byte(nil), s.data[off:off+int64(b.length)]...)
	if contains(s.cfg.corrupt, b.piece) {
		block[0] ^= 0xff
	}
	if s.cfg.delay > 0 {
		select {
		case <-time.After(s.cfg.delay):
		case <-s.end:
			return fmt.Errorf("closed")
		}
	}
	s.mu.Lock()
	s.served = append(s.served, b)
	s.mu.Unlock()
	return sc.write(wire.Message{ID: wire.IDPiece, Index: uint32(b.piece), Begin: b.begin, Block: block})
}

func contains(haystack []int, needle int) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}
	return false
}

// testTorrent builds a single-file metainfo over data. It goes through the
// real bencode round trip so the info-hash and piece hashes are genuine.
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

func testPayload(t *testing.T, n int) []byte {
	t.Helper()
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	return buf
}

type engineRun struct {
	eng *Engine
	out string
	err <-chan error
}

// startEngine runs one engine against the given peers in the background.
func startEngine(t *testing.T, meta *metainfo.MetaInfo, peers []tracker.Peer, tweak func(*Config)) *engineRun {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out.bin")
	cfg := Config{
		Meta:    meta,
		Port:    0,
		Output:  out,
		Tracker: &fakeTracker{peers: peers},
		Log:     t.Logf,
	}
	copy(cfg.PeerID[:], "-TC0001-testclientsub")
	if tweak != nil {
		tweak(&cfg)
	}
	eng := New(cfg)
	errs := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { errs <- eng.Run(ctx) }()
	t.Cleanup(cancel)
	return &engineRun{eng: eng, out: out, err: errs}
}

func (r *engineRun) wait(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case err := <-r.err:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(d):
		t.Fatalf("download did not finish within %s (livelock or deadlock?)", d)
	}
}

// uniqueServed counts how often each block was served across every seeder. A
// block served more than once is duplicated work; a block missing at the end
// is work that was lost.
func uniqueServed(seeders ...*fakeSeeder) map[[2]uint32]int {
	counts := make(map[[2]uint32]int)
	for _, s := range seeders {
		for _, b := range s.servedBlocks() {
			counts[b.key()]++
		}
	}
	return counts
}

func expectEachBlockOnce(t *testing.T, want int, seeders ...*fakeSeeder) {
	t.Helper()
	counts := uniqueServed(seeders...)
	if len(counts) != want {
		t.Fatalf("swarm served %d distinct blocks, want %d: %v", len(counts), want, counts)
	}
	for key, n := range counts {
		if n != 1 {
			t.Fatalf("block piece=%d begin=%d was served %d times, want exactly once", key[0], key[1], n)
		}
	}
}

func readOutput(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("downloaded %d bytes, want %d (identical: %v)", len(got), len(want), bytes.Equal(got, want))
	}
}

// Several peers must carry the download at the same time, not one after the
// other: the peak of peers with work in the request pipeline has to exceed 1.
func TestConcurrentPeersDownloadInParallel(t *testing.T) {
	const pieceLength = 64 * 1024
	data := testPayload(t, 3*pieceLength)
	meta := testTorrent(t, data, pieceLength)

	a := newFakeSeeder(t, "a", meta, data, seederConfig{advertise: []int{0, 1, 2}, delay: 5 * time.Millisecond})
	b := newFakeSeeder(t, "b", meta, data, seederConfig{advertise: []int{0, 1, 2}, delay: 5 * time.Millisecond})

	run := startEngine(t, meta, []tracker.Peer{a.peer(), b.peer()}, nil)
	run.wait(t, 30*time.Second)

	readOutput(t, run.out, data)
	stats := run.eng.Stats()
	if stats.PiecesDone != 3 {
		t.Fatalf("verified %d of 3 pieces", stats.PiecesDone)
	}
	if stats.PeersMaxActive < 2 {
		t.Fatalf("peak concurrent peers = %d, want >= 2 (peers used %d)", stats.PeersMaxActive, stats.PeersUsed)
	}
	if stats.PeersUsed != 2 {
		t.Fatalf("peers used = %d, want 2", stats.PeersUsed)
	}
	if stats.BlocksDuplicate != 0 {
		t.Fatalf("received %d duplicate blocks, want 0", stats.BlocksDuplicate)
	}
	if stats.BlocksStalled != 0 {
		t.Fatalf("expired %d block requests, want 0 on a healthy swarm", stats.BlocksStalled)
	}
	if a.servedCount() == 0 || b.servedCount() == 0 {
		t.Fatalf("each seeder must carry part of the download: a=%d b=%d", a.servedCount(), b.servedCount())
	}
	expectEachBlockOnce(t, 12, a, b)
	if v := append(a.violations(), b.violations()...); len(v) != 0 {
		t.Fatalf("protocol violations: %v", v)
	}

	// Each verified piece has to be announced to the peers, including the one
	// that did not serve it.
	expectHaveBroadcast(t, a)
	expectHaveBroadcast(t, b)
}

// expectHaveBroadcast requires the client to have told this seeder about a
// piece the seeder itself never served, which is what a have broadcast is for.
func expectHaveBroadcast(t *testing.T, s *fakeSeeder) {
	t.Helper()
	served := make(map[int]bool)
	for _, b := range s.servedBlocks() {
		served[int(b.key()[0])] = true
	}
	seen := s.havesSeen()
	for _, index := range seen {
		if !served[index] {
			return
		}
	}
	t.Fatalf("%s saw haves %v, none for a piece it did not serve", s.name, seen)
}

// The in-flight window is what backpressures a peer: with nothing answered, a
// peer must be holding exactly PipelineDepth requests and no more.
func TestPipelineDepthBoundsInFlightRequests(t *testing.T) {
	const pieceLength = 256 * 1024 // one piece, sixteen 16 KiB blocks
	data := testPayload(t, int(pieceLength))
	meta := testTorrent(t, data, pieceLength)

	silent := newFakeSeeder(t, "silent", meta, data, seederConfig{advertise: []int{0}, stall: []int{0}})

	eng := New(Config{
		Meta:          meta,
		Output:        filepath.Join(t.TempDir(), "out.bin"),
		Tracker:       &fakeTracker{peers: []tracker.Peer{silent.peer()}},
		Log:           t.Logf,
		PipelineDepth: 5,
		StallTimeout:  time.Minute,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	if err := eng.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want the context deadline", err)
	}

	if got := silent.requestCount(); got != 5 {
		t.Fatalf("peer was asked for %d blocks, want exactly the 5-block window", got)
	}
	stats := eng.Stats()
	if stats.BlocksRequested != 5 {
		t.Fatalf("engine issued %d requests, want 5", stats.BlocksRequested)
	}
	if stats.PeersActive != 1 {
		t.Fatalf("peers active = %d, want 1", stats.PeersActive)
	}
}

// A peer that has nothing we want must idle, not spin: the classic bug is a
// worker re-enqueueing a piece the peer cannot serve, forever at 100% CPU.
func TestPeerWithoutPiecesDoesNotStallTheDownload(t *testing.T) {
	const pieceLength = 64 * 1024
	data := testPayload(t, 2*pieceLength)
	meta := testTorrent(t, data, pieceLength)

	empty := newFakeSeeder(t, "empty", meta, data, seederConfig{})
	good := newFakeSeeder(t, "good", meta, data, seederConfig{advertise: []int{0, 1}})

	run := startEngine(t, meta, []tracker.Peer{empty.peer(), good.peer()}, nil)
	run.wait(t, 20*time.Second)

	readOutput(t, run.out, data)
	if empty.requestCount() != 0 {
		t.Fatalf("asked a peer that advertised nothing for %d blocks", empty.requestCount())
	}
	if good.servedCount() != 8 {
		t.Fatalf("good peer served %d blocks, want 8", good.servedCount())
	}
}

// A block nobody answers must be re-issued: ownership of the piece is handed
// back so a different peer can fetch the missing blocks.
func TestStalledRequestIsReissuedToAnotherPeer(t *testing.T) {
	const pieceLength = 64 * 1024
	data := testPayload(t, 2*pieceLength)
	meta := testTorrent(t, data, pieceLength)

	stalled := newFakeSeeder(t, "stalled", meta, data, seederConfig{advertise: []int{0, 1}, stall: []int{0, 1}})
	good := newFakeSeeder(t, "good", meta, data, seederConfig{advertise: []int{0, 1}})

	run := startEngine(t, meta, []tracker.Peer{stalled.peer(), good.peer()}, func(c *Config) {
		c.StallTimeout = 150 * time.Millisecond
	})
	run.wait(t, 20*time.Second)

	readOutput(t, run.out, data)
	stats := run.eng.Stats()
	if stats.BlocksStalled == 0 {
		t.Fatal("no request expired, so the stall path was never exercised")
	}
	if stalled.servedCount() != 0 {
		t.Fatalf("the stalling peer served %d blocks, want 0", stalled.servedCount())
	}
	if good.servedCount() != 8 {
		t.Fatalf("the good peer served %d blocks, want 8", good.servedCount())
	}
	expectEachBlockOnce(t, 8, stalled, good)
}

// A choke mid-piece must not lose the blocks already delivered: the next peer
// picks up only what is still missing.
func TestChokeMidTransferKeepsReceivedWork(t *testing.T) {
	const pieceLength = 64 * 1024
	data := testPayload(t, 3*pieceLength)
	meta := testTorrent(t, data, pieceLength)

	choker := newFakeSeeder(t, "choker", meta, data, seederConfig{advertise: []int{0, 1, 2}, chokeAfter: 2})
	friend := newFakeSeeder(t, "friend", meta, data, seederConfig{advertise: []int{0, 1, 2}})

	run := startEngine(t, meta, []tracker.Peer{choker.peer(), friend.peer()}, nil)
	run.wait(t, 20*time.Second)

	readOutput(t, run.out, data)
	stats := run.eng.Stats()
	if choker.servedCount() != 2 {
		t.Fatalf("the choker delivered %d blocks before choking, want 2", choker.servedCount())
	}
	if friend.servedCount() != 10 {
		t.Fatalf("the other peer delivered %d blocks, want 10 (the two delivered before the choke must be kept)", friend.servedCount())
	}
	if stats.PeersUsed != 2 {
		t.Fatalf("peers used = %d, want 2", stats.PeersUsed)
	}
	expectEachBlockOnce(t, 12, choker, friend)
}

// Corrupt data must cost the offending peer its session, and the piece must
// still complete from a peer that serves it honestly.
func TestBadBlockBlacklistsPeerAndRefetches(t *testing.T) {
	const pieceLength = 64 * 1024
	data := testPayload(t, 2*pieceLength)
	meta := testTorrent(t, data, pieceLength)

	corrupt := newFakeSeeder(t, "corrupt", meta, data, seederConfig{advertise: []int{0}, corrupt: []int{0}})
	honest := newFakeSeeder(t, "honest", meta, data, seederConfig{advertise: []int{1}})

	run := startEngine(t, meta, []tracker.Peer{corrupt.peer(), honest.peer()}, nil)
	go func() {
		// Once the corrupt peer has served a block, piece 0 is spoken for, so
		// the honest peer can advertise it without stealing the blame test.
		deadline := time.Now().Add(10 * time.Second)
		for corrupt.servedCount() == 0 && time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
		}
		honest.sendHave(0)
	}()
	run.wait(t, 20*time.Second)

	readOutput(t, run.out, data)
	stats := run.eng.Stats()
	if stats.BadPieces == 0 {
		t.Fatal("no piece failed verification, so the corrupt path was never exercised")
	}
	if corrupt.servedCount() == 0 {
		t.Fatal("the corrupt peer never served a block")
	}
	if honest.servedCount() != 8 {
		t.Fatalf("the honest peer served %d blocks, want 8", honest.servedCount())
	}
	if stats.PeersDropped == 0 {
		t.Fatal("no peer was dropped for serving bad data")
	}
	if !slices.Contains(stats.Blacklisted, corrupt.peer().Addr()) {
		t.Fatalf("blacklisted %v, want %s", stats.Blacklisted, corrupt.peer().Addr())
	}
}

// Only the final piece is short, and requests for it must respect that.
func TestShortLastPieceRequestsStayInBounds(t *testing.T) {
	const pieceLength = 64 * 1024
	data := testPayload(t, 2*pieceLength+1000)
	meta := testTorrent(t, data, pieceLength)
	if got := meta.PieceSize(2); got != 1000 {
		t.Fatalf("last piece is %d bytes, want 1000", got)
	}

	peer := newFakeSeeder(t, "peer", meta, data, seederConfig{advertise: []int{0, 1, 2}})
	run := startEngine(t, meta, []tracker.Peer{peer.peer()}, nil)
	run.wait(t, 20*time.Second)

	readOutput(t, run.out, data)
	if v := peer.violations(); len(v) != 0 {
		t.Fatalf("protocol violations: %v", v)
	}
	if peer.servedCount() != 9 {
		t.Fatalf("peer served %d blocks, want 9 (4 + 4 + 1)", peer.servedCount())
	}
}
