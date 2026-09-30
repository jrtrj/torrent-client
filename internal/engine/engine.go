package engine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"net"
	"time"

	"torrent-client/internal/metainfo"
	"torrent-client/internal/storage"
	"torrent-client/internal/tracker"
	"torrent-client/internal/wire"
)

const (
	// pipelineDepth is how many block requests stay in flight per connection.
	// It is a minimum for the concurrency ticket; sequential downloads are
	// still faster with a few requests outstanding.
	pipelineDepth = 8

	dialTimeout      = 10 * time.Second
	handshakeTimeout = 10 * time.Second
	// idleTimeout bounds the wait for any message from a peer before the
	// connection counts as stalled. It is also the write deadline.
	idleTimeout = 30 * time.Second
	// noProgressTimeout fails a download that cannot move forward, so a
	// tracker with no peers (or all-dead peers) surfaces as an error instead
	// of a hang.
	noProgressTimeout = 2 * time.Minute
	// swarmRetryDelay is the pause between announce rounds while waiting for
	// peers to appear.
	swarmRetryDelay = 1 * time.Second
)

// errPieceFailed marks a piece that did not match its hash. The peer that sent
// it is not trusted again this session.
var errPieceFailed = errors.New("engine: piece failed verification")

// Config is everything one download needs. The tracker is injected so the
// engine never constructs transports itself.
type Config struct {
	Meta    *metainfo.MetaInfo
	PeerID  [20]byte
	Port    uint16
	Output  string
	Tracker tracker.Tracker

	// Log receives one line per milestone. A nil Log discards them.
	Log func(format string, args ...any)
}

// Engine downloads a torrent's content from a swarm, one peer at a time.
type Engine struct {
	cfg    Config
	store  *storage.Storage
	have   []byte
	peers  map[string]bool // peers to skip for the rest of the session
	done   int64           // verified bytes
	rounds int
}

// New builds an engine for one download.
func New(cfg Config) *Engine {
	return &Engine{cfg: cfg, peers: make(map[string]bool)}
}

func (e *Engine) logf(format string, args ...any) {
	if e.cfg.Log != nil {
		e.cfg.Log(format, args...)
	}
}

// Run downloads every piece and returns once the content is complete. It is
// deliberately sequential: scheduling many peers across pieces is the
// concurrency ticket's job.
func (e *Engine) Run(ctx context.Context) error {
	meta := e.cfg.Meta
	if meta.Multifile() {
		return errors.New("engine: multi-file torrents are not supported yet")
	}

	store, err := storage.Open(e.cfg.Output, meta.Info.PieceLength, meta.TotalLength())
	if err != nil {
		return fmt.Errorf("engine: open output %s: %w", e.cfg.Output, err)
	}
	defer store.Close()
	e.store = store
	e.have = wire.NewBitfield(meta.PieceCount())

	// A fatal reply here (a tracker that rejects our info-hash) is the one
	// announce failure worth stopping for; transient ones are retried below.
	if _, err := e.announce(ctx, tracker.EventStarted); err != nil {
		if tracker.IsFatal(err) {
			return err
		}
		e.logf("engine: initial announce failed, will retry: %v", err)
	}

	if err := e.download(ctx); err != nil {
		return err
	}

	if _, err := e.announce(ctx, tracker.EventCompleted); err != nil {
		e.logf("engine: completed announce failed: %v", err)
	}
	e.logf("engine: downloaded %d bytes in %d piece(s)", e.done, meta.PieceCount())
	return nil
}

func (e *Engine) download(ctx context.Context) error {
	lastProgress := time.Now()
	for !e.complete() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Since(lastProgress) > noProgressTimeout {
			return fmt.Errorf("engine: no progress for %s (downloaded %d of %d bytes)",
				noProgressTimeout, e.done, e.cfg.Meta.TotalLength())
		}

		e.rounds++
		peers, err := e.announce(ctx, "")
		if err != nil {
			e.logf("engine: announce failed: %v", err)
		}
		before := e.done
		for _, p := range peers {
			if e.complete() || ctx.Err() != nil {
				break
			}
			if e.peers[p.Addr()] {
				continue
			}
			if err := e.downloadFromPeer(ctx, p); err != nil {
				e.logf("engine: peer %s: %v", p.Addr(), err)
				if errors.Is(err, errPieceFailed) {
					e.peers[p.Addr()] = true
				}
			}
		}
		if e.complete() {
			break
		}
		if e.done == before {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(swarmRetryDelay):
			}
		}
	}
	return nil
}

func (e *Engine) announce(ctx context.Context, ev tracker.Event) ([]tracker.Peer, error) {
	resp, err := tracker.AnnounceWithRetry(ctx, e.cfg.Tracker, e.announceRequest(ev), tracker.DefaultRetry)
	if err != nil {
		return nil, err
	}
	// Filter out ports we could never dial rather than failing on them.
	peers := resp.Peers[:0]
	for _, p := range resp.Peers {
		if p.Port != 0 && p.IP != nil {
			peers = append(peers, p)
		}
	}
	return peers, nil
}

func (e *Engine) announceRequest(ev tracker.Event) tracker.AnnounceRequest {
	total := e.cfg.Meta.TotalLength()
	return tracker.AnnounceRequest{
		InfoHash:   e.cfg.Meta.InfoHash,
		PeerID:     e.cfg.PeerID,
		Port:       e.cfg.Port,
		Downloaded: e.done,
		Left:       total - e.done,
		Event:      ev,
		NumWant:    50,
	}
}

// complete reports whether every piece has been verified and written.
func (e *Engine) complete() bool {
	return wire.BitfieldCount(e.have, e.cfg.Meta.PieceCount()) == e.cfg.Meta.PieceCount()
}

// downloadFromPeer connects to one peer and pulls every piece it has that we
// still need, in index order.
func (e *Engine) downloadFromPeer(ctx context.Context, p tracker.Peer) error {
	dialer := &net.Dialer{Timeout: dialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", p.Addr())
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return err
	}
	if _, err := conn.Write(wire.NewHandshake(e.cfg.Meta.InfoHash, e.cfg.PeerID).Encode()); err != nil {
		return err
	}
	if _, err := wire.ReadHandshake(conn, e.cfg.Meta.InfoHash); err != nil {
		return err
	}

	pc := &peerConn{
		conn:       conn,
		r:          bufio.NewReader(conn),
		pieceCount: e.cfg.Meta.PieceCount(),
		bits:       wire.NewBitfield(e.cfg.Meta.PieceCount()),
		choked:     true,
	}
	if err := wire.Write(conn, wire.Message{ID: wire.IDInterested}); err != nil {
		return err
	}
	e.logf("engine: connected to %s", p.Addr())

	for index := 0; index < pc.pieceCount; index++ {
		if wire.BitfieldHas(e.have, index) {
			continue
		}
		// We only know what a peer has once it tells us, so the first wanted
		// piece waits for its bitfield or first have.
		if err := pc.learnAvailability(); err != nil {
			return err
		}
		if !wire.BitfieldHas(pc.bits, index) {
			continue
		}
		if err := e.downloadPiece(pc, index); err != nil {
			return err
		}
	}
	return nil
}

// downloadPiece fetches one whole piece, verifies it, and writes it. Blocks
// are requested in order with a small pipeline; a choke drops the in-flight
// requests so anything still missing is asked for again.
func (e *Engine) downloadPiece(pc *peerConn, index int) error {
	size := e.cfg.Meta.PieceSize(index)
	blocks := blockRanges(size)
	buf := make([]byte, size)
	got, inflight, next := 0, 0, 0

	for got < len(blocks) {
		for !pc.choked && inflight < pipelineDepth && next < len(blocks) {
			b := &blocks[next]
			if b.received || b.inflight {
				next++
				continue
			}
			req := wire.Message{ID: wire.IDRequest, Index: uint32(index), Begin: b.begin, Length: b.length}
			if err := connSetWriteDeadline(pc.conn); err != nil {
				return err
			}
			if err := wire.Write(pc.conn, req); err != nil {
				return err
			}
			b.inflight = true
			inflight++
			next++
		}

		if err := connSetReadDeadline(pc.conn); err != nil {
			return err
		}
		m, err := pc.read()
		if err != nil {
			return err
		}

		switch m.ID {
		case wire.IDPiece:
			if int(m.Index) != index {
				return fmt.Errorf("engine: peer sent piece %d while we were fetching piece %d", m.Index, index)
			}
			b := findBlock(blocks, m.Begin)
			if b == nil || b.received {
				continue // duplicate or unrequested block: drop it
			}
			if int64(m.Begin)+int64(len(m.Block)) > size {
				return fmt.Errorf("engine: block %d+%d overruns piece %d (%d bytes)", m.Begin, len(m.Block), index, size)
			}
			copy(buf[m.Begin:], m.Block)
			b.received = true
			if b.inflight {
				b.inflight = false
				inflight--
			}
			got++
		case wire.IDChoke:
			pc.choked = true
			inflight = 0
			next = 0
			for i := range blocks {
				blocks[i].inflight = false
			}
		case wire.IDUnchoke:
			pc.choked = false
		}
	}

	sum := sha1.Sum(buf)
	if !bytes.Equal(sum[:], e.cfg.Meta.PieceHash(index)) {
		return fmt.Errorf("%w: piece %d", errPieceFailed, index)
	}
	if err := e.store.WritePiece(index, buf); err != nil {
		return err
	}
	wire.BitfieldSet(e.have, index)
	e.done += int64(size)
	e.logf("engine: piece %d/%d verified (%d/%d bytes)", index+1, pc.pieceCount, e.done, e.cfg.Meta.TotalLength())

	if err := connSetWriteDeadline(pc.conn); err != nil {
		return err
	}
	return wire.Write(pc.conn, wire.Message{ID: wire.IDHave, Index: uint32(index)})
}

// peerConn is one peer connection's read side: it tracks choke state and the
// peer's availability so the download loop can stay in one goroutine.
type peerConn struct {
	conn       net.Conn
	r          *bufio.Reader
	pieceCount int
	bits       []byte
	choked     bool
	learned    bool
}

// read decodes one message and folds bitfield/have into the peer's map.
// A bitfield longer than the torrent is connection-fatal: it cannot be
// trusted to be about our torrent.
func (pc *peerConn) read() (wire.Message, error) {
	m, err := wire.Decode(pc.r)
	if err != nil {
		return m, err
	}
	switch m.ID {
	case wire.IDBitfield:
		if len(m.Bitfield) > len(pc.bits) {
			return m, fmt.Errorf("engine: peer bitfield is %d bytes but the torrent has %d pieces", len(m.Bitfield), pc.pieceCount)
		}
		copy(pc.bits, m.Bitfield)
		pc.learned = true
	case wire.IDHave:
		if int(m.Index) >= pc.pieceCount {
			return m, fmt.Errorf("engine: peer have index %d is out of range for %d pieces", m.Index, pc.pieceCount)
		}
		wire.BitfieldSet(pc.bits, int(m.Index))
		pc.learned = true
	}
	return m, nil
}

// learnAvailability reads until the peer has told us what it holds. Choke
// state is folded in so the caller does not have to re-read it.
func (pc *peerConn) learnAvailability() error {
	for !pc.learned {
		if err := connSetReadDeadline(pc.conn); err != nil {
			return err
		}
		m, err := pc.read()
		if err != nil {
			return err
		}
		switch m.ID {
		case wire.IDChoke:
			pc.choked = true
		case wire.IDUnchoke:
			pc.choked = false
		}
	}
	return nil
}

// blockRange is one block request within a piece.
type blockRange struct {
	begin    uint32
	length   uint32
	received bool
	inflight bool
}

// blockRanges splits a piece into requests of at most wire.BlockSize. The
// final block is short when the piece does not divide evenly.
func blockRanges(size int64) []blockRange {
	if size <= 0 {
		return nil
	}
	ranges := make([]blockRange, 0, (size+wire.BlockSize-1)/wire.BlockSize)
	for begin := int64(0); begin < size; begin += wire.BlockSize {
		length := int64(wire.BlockSize)
		if begin+length > size {
			length = size - begin
		}
		ranges = append(ranges, blockRange{begin: uint32(begin), length: uint32(length)})
	}
	return ranges
}

func findBlock(blocks []blockRange, begin uint32) *blockRange {
	for i := range blocks {
		if blocks[i].begin == begin {
			return &blocks[i]
		}
	}
	return nil
}

func connSetReadDeadline(c net.Conn) error {
	return c.SetReadDeadline(time.Now().Add(idleTimeout))
}

func connSetWriteDeadline(c net.Conn) error {
	return c.SetWriteDeadline(time.Now().Add(idleTimeout))
}
