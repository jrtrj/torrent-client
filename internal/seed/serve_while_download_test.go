package seed

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"torrent-client/internal/engine"
	"torrent-client/internal/metainfo"
	"torrent-client/internal/storage"
	"torrent-client/internal/tracker"
	"torrent-client/internal/wire"
)

// This test is the concurrency proof: a real download engine writes verified
// pieces into a content store while the seed server reads from the same store
// and uploads pieces to another peer. Run under -race, it exercises the two
// paths touching one store at once.

// engineSource adapts the engine and its store to the upload path the same way
// the client does: the engine decides what is held, the store hands out bytes.
type engineSource struct {
	eng   *engine.Engine
	store *storage.Storage
}

func (s *engineSource) Have(index int) bool { return s.eng.Have(index) }

func (s *engineSource) HaveBitfield() []byte { return s.eng.HaveBitfield() }

func (s *engineSource) ReadBlock(index int, begin, length uint32) ([]byte, error) {
	return s.store.ReadBlock(index, begin, length)
}

// fakeTracker hands the engine one peer and never changes the swarm.
type fakeTracker struct{ peers []tracker.Peer }

func (f *fakeTracker) Announce(context.Context, tracker.AnnounceRequest) (tracker.AnnounceResponse, error) {
	return tracker.AnnounceResponse{Interval: time.Hour, Peers: f.peers}, nil
}

// slowSeeder is a minimal seeding peer: it serves the whole fixture but waits
// before every block, so the download window is wide enough for another peer to
// fetch from us while we are still fetching.
type slowSeeder struct {
	meta  *metainfo.MetaInfo
	data  []byte
	delay time.Duration

	ln   net.Listener
	done chan struct{}
	wg   sync.WaitGroup

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func startSlowSeeder(t *testing.T, meta *metainfo.MetaInfo, data []byte, delay time.Duration) *slowSeeder {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &slowSeeder{
		meta: meta, data: data, delay: delay,
		ln: ln, done: make(chan struct{}), conns: make(map[net.Conn]struct{}),
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns[conn] = struct{}{}
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer func() {
					s.mu.Lock()
					delete(s.conns, conn)
					s.mu.Unlock()
				}()
				s.serveConn(conn)
			}()
		}
	}()
	t.Cleanup(func() {
		close(s.done)
		ln.Close()
		s.mu.Lock()
		for c := range s.conns {
			c.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return s
}

func (s *slowSeeder) peer() tracker.Peer {
	a := s.ln.Addr().(*net.TCPAddr)
	return tracker.Peer{IP: a.IP, Port: uint16(a.Port)}
}

func (s *slowSeeder) serveConn(conn net.Conn) {
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return
	}
	var id [20]byte
	copy(id[:], "-SLOWSEEDER000000001")
	if _, err := conn.Write(wire.NewHandshake(s.meta.InfoHash, id).Encode()); err != nil {
		return
	}
	if _, err := wire.ReadHandshake(conn, s.meta.InfoHash); err != nil {
		return
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return
	}
	r := bufio.NewReader(conn)
	if err := wire.Write(conn, wire.Message{ID: wire.IDBitfield, Bitfield: wire.BitfieldComplete(s.meta.PieceCount())}); err != nil {
		return
	}
	for {
		m, err := wire.Decode(r)
		if err != nil {
			return
		}
		switch m.ID {
		case wire.IDInterested:
			if err := wire.Write(conn, wire.Message{ID: wire.IDUnchoke}); err != nil {
				return
			}
		case wire.IDRequest:
			if err := s.serveBlock(conn, m); err != nil {
				return
			}
		}
	}
}

func (s *slowSeeder) serveBlock(conn net.Conn, m wire.Message) error {
	index := int(m.Index)
	if index < 0 || index >= s.meta.PieceCount() || m.Length == 0 || m.Length > MaxRequestLength ||
		int64(m.Begin)+int64(m.Length) > s.meta.PieceSize(index) {
		return fmt.Errorf("bad request")
	}
	off := int64(index)*s.meta.Info.PieceLength + int64(m.Begin)
	block := append([]byte(nil), s.data[off:off+int64(m.Length)]...)
	select {
	case <-time.After(s.delay):
	case <-s.done:
		return errors.New("closed")
	}
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return wire.Write(conn, wire.Message{ID: wire.IDPiece, Index: m.Index, Begin: m.Begin, Block: block})
}

// fetchResult is what a fetching peer observed: the bytes it collected and
// when it completed its first piece.
type fetchResult struct {
	first time.Time
	data  []byte
	err   error
}

// fetchWhileGrowing connects to srv, records every have it is told about, and
// fetches every piece as it becomes available, byte-checking as it goes. It
// returns once it holds every piece or the deadline passes.
func fetchWhileGrowing(srv *Server, meta *metainfo.MetaInfo, deadline time.Time) fetchResult {
	var res fetchResult
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		res.err = err
		return res
	}
	defer conn.Close()
	r := bufio.NewReader(conn)

	var id [20]byte
	copy(id[:], "-FETCHER000000000001")
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		res.err = err
		return res
	}
	if _, err := conn.Write(wire.NewHandshake(meta.InfoHash, id).Encode()); err != nil {
		res.err = err
		return res
	}
	if err := conn.SetReadDeadline(deadline); err != nil {
		res.err = err
		return res
	}
	if _, err := wire.ReadHandshake(conn, meta.InfoHash); err != nil {
		res.err = err
		return res
	}

	read := func() (wire.Message, error) {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return wire.Message{}, err
		}
		return wire.Decode(r)
	}
	send := func(m wire.Message) error {
		if err := conn.SetWriteDeadline(deadline); err != nil {
			return err
		}
		return wire.Write(conn, m)
	}

	have := make([]bool, meta.PieceCount())
	done := make([]bool, meta.PieceCount())
	got := make([]byte, meta.TotalLength())
	remaining := meta.PieceCount()

	// The server writes its bitfield first; fold it in.
	m, err := read()
	if err != nil {
		res.err = err
		return res
	}
	if m.ID == wire.IDBitfield {
		for i := range have {
			have[i] = wire.BitfieldHas(m.Bitfield, i)
		}
	}
	if err := send(wire.Message{ID: wire.IDInterested}); err != nil {
		res.err = err
		return res
	}

	// readPiece skips the unchoke and any haves until it sees the block reply.
	readPiece := func() (wire.Message, error) {
		for {
			m, err := read()
			if err != nil {
				return wire.Message{}, err
			}
			switch m.ID {
			case wire.IDHave:
				if int(m.Index) < len(have) {
					have[m.Index] = true
				}
			case wire.IDPiece:
				return m, nil
			case wire.IDUnchoke, wire.IDChoke, wire.IDBitfield:
			default:
				return wire.Message{}, fmt.Errorf("unexpected message id %d", m.ID)
			}
		}
	}

	for remaining > 0 {
		if time.Now().After(deadline) {
			res.err = errors.New("fetch timed out")
			return res
		}
		progressed := false
		for i := range have {
			if !have[i] || done[i] {
				continue
			}
			size := meta.PieceSize(i)
			for begin := int64(0); begin < size; begin += wire.BlockSize {
				length := min(int64(wire.BlockSize), size-begin)
				if err := send(wire.Message{ID: wire.IDRequest, Index: uint32(i), Begin: uint32(begin), Length: uint32(length)}); err != nil {
					res.err = err
					return res
				}
				reply, err := readPiece()
				if err != nil {
					res.err = err
					return res
				}
				if int(reply.Index) != i || reply.Begin != uint32(begin) {
					res.err = fmt.Errorf("piece reply %d+%d, want %d+%d", reply.Index, reply.Begin, i, begin)
					return res
				}
				copy(got[int64(i)*meta.Info.PieceLength+begin:], reply.Block)
			}
			done[i] = true
			remaining--
			progressed = true
			if res.first.IsZero() {
				res.first = time.Now()
			}
		}
		if remaining == 0 || progressed {
			continue
		}
		// Nothing we know about is left: wait to be told about a new piece.
		m, err := read()
		if err != nil {
			res.err = err
			return res
		}
		if m.ID == wire.IDHave && int(m.Index) < len(have) {
			have[m.Index] = true
		}
	}
	res.data = got
	return res
}

// TestServesPiecesWhileDownloading runs a real engine and the seed server in
// one process, sharing a content store: the engine is downloading from a slow
// peer while another peer fetches verified pieces from us. It asserts the
// fetcher got byte-correct data, that this happened while the download was
// still running, and that the name/IP layer never races (run under -race).
func TestServesPiecesWhileDownloading(t *testing.T) {
	const pieces = 6
	data := testPayload(t, pieces*testPieceLength)
	meta := testTorrent(t, data, testPieceLength)

	out := filepath.Join(t.TempDir(), "out.bin")
	store, err := storage.Open(out, meta.Info.PieceLength, meta.TotalLength())
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	defer store.Close()

	// 20 ms per block over 24 blocks keeps the download running for ~0.5 s.
	seeder := startSlowSeeder(t, meta, data, 20*time.Millisecond)
	eng := engine.New(engine.Config{
		Meta:    meta,
		PeerID:  testPeerID(),
		Output:  out,
		Store:   store,
		Tracker: &fakeTracker{peers: []tracker.Peer{seeder.peer()}},
		Log:     t.Logf,
	})
	src := &engineSource{eng: eng, store: store}
	srv := newTestServer(t, meta, src)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type runResult struct {
		at  time.Time
		err error
	}
	runDone := make(chan runResult, 1)
	go func() {
		err := eng.Run(ctx)
		runDone <- runResult{at: time.Now(), err: err}
	}()

	res := fetchWhileGrowing(srv, meta, time.Now().Add(20*time.Second))

	var run runResult
	select {
	case run = <-runDone:
	case <-time.After(20 * time.Second):
		t.Fatal("download did not finish")
	}
	if run.err != nil {
		t.Fatalf("engine.Run: %v", run.err)
	}
	if res.err != nil {
		t.Fatalf("fetcher: %v", res.err)
	}
	if !bytes.Equal(res.data, data) {
		t.Fatalf("fetched data differs from the fixture")
	}
	if res.first.IsZero() {
		t.Fatal("the fetcher never completed a piece")
	}
	if !res.first.Before(run.at) {
		t.Fatalf("first piece fetched at %s, but the download only finished at %s: "+
			"serving during the download was not exercised", res.first, run.at)
	}
	if srv.BlocksServed() == 0 {
		t.Fatal("the server reported no blocks served")
	}
	if up := srv.Uploaded(); up != meta.TotalLength() {
		t.Fatalf("uploaded %d bytes, want %d", up, meta.TotalLength())
	}

	// The download itself must still be intact.
	onDisk, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !bytes.Equal(onDisk, data) {
		t.Fatal("the completed download differs from the fixture")
	}
}
