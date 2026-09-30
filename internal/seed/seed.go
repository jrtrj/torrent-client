// Package seed serves verified pieces back to the swarm. cmd/seed and the real
// client both drive it, so the serving rules live in one place.
//
// One torrent per Server: the handshake is checked against that info-hash and
// anything else is dropped without a reply. We serve whatever the Source says is
// held, so a peer that finds us mid-download gets the pieces verified so far.
//
// uploadSlots peers are unchoked at once and hold their slot until they lose
// interest or disconnect. No tit-for-tat, no rotation - it bounds state, it is
// not a fairness claim. Someone who arrives with every slot taken stays choked
// for the session.
//
// Blocks are read straight out of the store with pread at request time and never
// cached, so RAM does not scale with torrent size, and serving a piece while
// another is being written is safe at the OS level. An optional token bucket
// caps upload; nil or zero means unlimited.
package seed

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"torrent-client/internal/bencode"
	"torrent-client/internal/metainfo"
	"torrent-client/internal/ratelimit"
	"torrent-client/internal/wire"
)

const (
	// handshakeTimeout bounds the opening exchange with a new inbound peer.
	handshakeTimeout = 10 * time.Second
	// MaxRequestLength is the classic protocol ceiling for a single block
	// request. A longer request is a protocol violation, not a big block.
	MaxRequestLength = 128 * 1024
	// uploadSlots is how many inbound peers may be unchoked at once. It bounds
	// both the number of peers served progressively and the live read buffers.
	uploadSlots = 4
	// maxConns caps inbound connections so a flood of idle peers cannot grow
	// goroutines without bound.
	maxConns = 64
	// writeTimeout bounds one frame write, so a peer that stops reading cannot
	// pin a serving goroutine forever.
	writeTimeout = 30 * time.Second
	// How often a connection looks for pieces that became held after it
	// connected. The downloader publishes completions with no event to subscribe
	// to, so we poll.
	haveInterval = 100 * time.Millisecond
)

// Source is the content side of serving: the pieces we hold and a reader for
// their bytes. The engine's verified-piece set plus the content store satisfy
// it in the real client; a fully-downloaded fixture satisfies it in cmd/seed.
type Source interface {
	// Have reports whether piece index is complete, hash-verified, and so
	// safe to upload. It must be safe to call concurrently.
	Have(index int) bool
	// Everything held right now, as one snapshot. The advertise tick takes it
	// once instead of calling Have per piece.
	HaveBitfield() []byte
	// ReadBlock returns length bytes at begin within piece index. The server
	// has already checked that the span fits the piece.
	ReadBlock(index int, begin, length uint32) ([]byte, error)
}

// Config is one server's settings.
type Config struct {
	// Meta is the torrent this server serves. The server accepts handshakes
	// for this info-hash only: a per-torrent allow-list that is exactly the
	// torrent we hold.
	Meta *metainfo.MetaInfo
	// Source reports which pieces are servable and reads their bytes.
	Source Source
	// PeerID is the id sent in our handshake.
	PeerID [20]byte
	// Listen is the TCP address to bind, e.g. "127.0.0.1:6881". A port of 0
	// picks a free one; Addr reports what was bound.
	Listen string
	// Log receives one line per served block and per dropped connection. A nil
	// Log discards them. Calls are serialised.
	Log func(format string, args ...any)
	// Limiter caps the upload rate; nil or a zero rate means unlimited. The
	// caller owns it, so the download side can be capped independently.
	Limiter *ratelimit.Limiter
	// ServeDelay sleeps before answering each block request, widening the
	// serving window. It exists for the swarm tests; production leaves it 0.
	ServeDelay time.Duration
}

// Server accepts inbound peers and uploads verified pieces to them.
type Server struct {
	cfg        Config
	meta       *metainfo.MetaInfo
	src        Source
	pieceCount int

	ln    net.Listener
	slots chan struct{}

	// Cancelled by Close, and folded into the context the limiter sees: a
	// goroutine asleep in a bandwidth wait does not notice its socket closing,
	// and would hold Close open until its turn came round.
	closing     context.Context
	closeCancel context.CancelFunc

	uploadedBytes atomic.Int64
	uploadedBlock atomic.Int64

	logMu sync.Mutex

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

// New binds the listening socket and returns a server ready for Serve. It
// binds eagerly so the caller can announce the port it actually listens on.
func New(cfg Config) (*Server, error) {
	if cfg.Meta == nil {
		return nil, errors.New("seed: nil metainfo")
	}
	if cfg.Source == nil {
		return nil, errors.New("seed: nil source")
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("seed: listen %s: %w", cfg.Listen, err)
	}
	closing, closeCancel := context.WithCancel(context.Background())
	return &Server{
		cfg:         cfg,
		meta:        cfg.Meta,
		src:         cfg.Source,
		pieceCount:  cfg.Meta.PieceCount(),
		ln:          ln,
		slots:       make(chan struct{}, uploadSlots),
		closing:     closing,
		closeCancel: closeCancel,
		conns:       make(map[net.Conn]struct{}),
	}, nil
}

// Addr is the bound listen address.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Port is the bound TCP port, or 0 if the listener is not a TCP socket.
func (s *Server) Port() uint16 {
	if a, ok := s.ln.Addr().(*net.TCPAddr); ok {
		return uint16(a.Port)
	}
	return 0
}

// Uploaded is the number of content bytes served this session. It is the
// counter a caller announces to the tracker.
func (s *Server) Uploaded() int64 { return s.uploadedBytes.Load() }

// BlocksServed is how many block responses the server has written.
func (s *Server) BlocksServed() int64 { return s.uploadedBlock.Load() }

// Serve accepts inbound peers until ctx is cancelled or Close is called. It
// closes the listener and every open connection on the way out, so a returned
// Serve leaves nothing running.
func (s *Server) Serve(ctx context.Context) error {
	// The serving goroutines get a context that Close also cancels, so a
	// bandwidth wait is released on both shutdown paths.
	serveCtx, cancelServe := context.WithCancel(ctx)
	defer cancelServe()
	stopClosing := context.AfterFunc(s.closing, cancelServe)
	defer stopClosing()

	closer := make(chan struct{})
	go func() {
		select {
		case <-serveCtx.Done():
		case <-closer:
		}
		s.ln.Close()
	}()
	defer close(closer)
	defer s.Close()

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			// A cancelled context or our own Close closes the listener; that
			// is a clean shutdown, not an accept failure.
			if serveCtx.Err() != nil || s.isClosed() {
				return nil
			}
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.serveConn(serveCtx, conn)
		}()
	}
}

// Close stops accepting, drops every open connection, and waits for the
// serving goroutines. It is idempotent.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	// Release anyone waiting for bandwidth before waiting for the serving
	// goroutines: closing the sockets below does not interrupt a limiter wait.
	s.closeCancel()
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	for _, c := range conns {
		c.Close()
	}
	s.ln.Close()
	s.wg.Wait()
	return nil
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Server) track(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.conns) >= maxConns {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) untrack(c net.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

func (s *Server) logf(format string, args ...any) {
	if s.cfg.Log == nil {
		return
	}
	s.logMu.Lock()
	s.cfg.Log(format, args...)
	s.logMu.Unlock()
}

// acquire takes an upload slot without blocking; it reports whether one was
// free.
func (s *Server) acquire() bool {
	select {
	case s.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// errDrop marks a protocol violation: the request must be refused and the
// connection closed rather than answered.
var errDrop = errors.New("protocol violation")

// serveConn runs one inbound peer: handshake, bitfield, then a request loop.
func (s *Server) serveConn(ctx context.Context, nc net.Conn) {
	defer nc.Close()
	if !s.track(nc) {
		return // over the connection cap
	}
	defer s.untrack(nc)

	if err := nc.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return
	}
	// Write our handshake first; the protocol does not order the two writes
	// and a real peer tolerates either.
	if _, err := nc.Write(wire.NewHandshake(s.meta.InfoHash, s.cfg.PeerID).Encode()); err != nil {
		return
	}
	// The swarm gate. Anything that is not this torrent's info-hash is dropped
	// without a reply. How much of the torrent we actually hold is a per-piece
	// question further down, not here - that is what lets a peer fetch from us
	// mid-download.
	if _, err := wire.ReadHandshake(nc, s.meta.InfoHash); err != nil {
		s.logf("dropped inbound peer %s: %v", nc.RemoteAddr(), err)
		return
	}
	// Clear the deadline: a seeding connection sits idle between requests.
	if err := nc.SetDeadline(time.Time{}); err != nil {
		return
	}

	c := &inbound{nc: nc, r: bufio.NewReader(nc)}

	// The opening bitfield is a snapshot of what we hold now; the advertiser
	// below reports pieces that appear later. It is copied so the message we
	// write never aliases the Source's slice.
	have := wire.NewBitfield(s.pieceCount)
	copy(have, s.src.HaveBitfield())
	if err := c.write(wire.Message{ID: wire.IDBitfield, Bitfield: have}); err != nil {
		return
	}

	done := make(chan struct{})
	defer close(done)
	go s.advertiseHave(ctx, c, have, done)

	// unchoked tracks whether this connection holds an upload slot, so the
	// slot is released exactly once on every exit path.
	unchoked := false
	defer func() {
		if unchoked {
			<-s.slots
		}
	}()

	for {
		m, err := wire.Decode(c.r)
		if err != nil {
			return
		}
		switch m.ID {
		case wire.IDInterested:
			if !unchoked && s.acquire() {
				unchoked = true
				if err := c.write(wire.Message{ID: wire.IDUnchoke}); err != nil {
					return
				}
			}
		case wire.IDNotInterested:
			if unchoked {
				<-s.slots
				unchoked = false
				if err := c.write(wire.Message{ID: wire.IDChoke}); err != nil {
					return
				}
			}
		case wire.IDRequest:
			// A choked peer is not served; that is the whole point of the
			// slot policy.
			if !unchoked {
				continue
			}
			if err := s.serveRequest(ctx, c, m); err != nil {
				if ctx.Err() != nil || s.isClosed() {
					// The server is going away: the request was not
					// refused, so this is not a peer to report.
					return
				}
				s.logf("dropped inbound peer %s: %v", nc.RemoteAddr(), err)
				return
			}
		case wire.IDExtended:
			if err := s.serveExtension(c, m); err != nil {
				if ctx.Err() != nil || s.isClosed() {
					return
				}
				s.logf("dropped inbound peer %s: %v", nc.RemoteAddr(), err)
				return
			}
		default:
			// Keep-alives, choke/unchoke/have/bitfield/cancel from the peer,
			// and extension ids we do not implement do not change what we
			// serve.
		}
	}
}

// ut_metadata (BEP 9) ids for the metadata we serve. Id 0 is reserved for the
// extension handshake itself, so ut_metadata takes the id we advertise.
const (
	extensionHandshakeID = 0
	ourUTMetadataID      = 1
	metadataPieceSize    = 16 * 1024
)

// ut_metadata message types.
const (
	msgRequest = 0
	msgData    = 1
	msgReject  = 2
)

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

// Tells peers we can serve the metadata and how big it is. We answer rather
// than volunteer: every client opens with this anyway, and an unsolicited frame
// just changes the sequence all the other peers have to tolerate.
func (s *Server) sendExtensionHandshake(c *inbound) error {
	var buf bytes.Buffer
	err := bencode.Marshal(&buf, extensionHandshake{
		M:            map[string]int{"ut_metadata": ourUTMetadataID},
		MetadataSize: int64(len(s.meta.RawInfo)),
		V:            "torrent-client 0.1",
	})
	if err != nil {
		return err
	}
	return c.write(wire.Message{
		ID:       wire.IDExtended,
		Extended: append([]byte{extensionHandshakeID}, buf.Bytes()...),
	})
}

// serveExtension answers the extension protocol (BEP 10). Only the handshake
// and ut_metadata requests are acted on; anything else is ignored, as the BEP
// requires, so an extension we do not know cannot cost a peer its connection.
func (s *Server) serveExtension(c *inbound, m wire.Message) error {
	if len(m.Extended) == 0 {
		return nil
	}
	extID, body := m.Extended[0], m.Extended[1:]
	switch extID {
	case extensionHandshakeID:
		return s.sendExtensionHandshake(c)
	case ourUTMetadataID:
		return s.serveMetadataRequest(c, body)
	default:
		return nil
	}
}

// A request past the end gets the protocol's own reject rather than a dropped
// connection. Asking for the wrong piece is confusion, not hostility.
func (s *Server) serveMetadataRequest(c *inbound, body []byte) error {
	var msg metadataMessage
	if err := bencode.Unmarshal(bytes.NewReader(body), &msg); err != nil {
		// A malformed request is not worth dropping the peer over.
		return nil
	}
	if msg.MsgType != msgRequest {
		return nil
	}

	info := s.meta.RawInfo
	start := msg.Piece * metadataPieceSize
	if len(info) == 0 || msg.Piece < 0 || start >= len(info) {
		return c.writeMetadata(metadataMessage{MsgType: msgReject, Piece: msg.Piece}, nil)
	}
	end := start + metadataPieceSize
	if end > len(info) {
		end = len(info)
	}
	return c.writeMetadata(metadataMessage{
		MsgType:   msgData,
		Piece:     msg.Piece,
		TotalSize: len(info),
	}, info[start:end])
}

// writeMetadata sends a ut_metadata message. A data message is a bencoded
// dictionary immediately followed by the raw piece, so the payload is assembled
// here rather than encoded from a single struct.
func (c *inbound) writeMetadata(msg metadataMessage, data []byte) error {
	var buf bytes.Buffer
	if err := bencode.Marshal(&buf, msg); err != nil {
		return err
	}
	payload := append([]byte{ourUTMetadataID}, buf.Bytes()...)
	payload = append(payload, data...)
	return c.write(wire.Message{ID: wire.IDExtended, Extended: payload})
}

// A request for a piece we do not hold yet is ignored, not refused: peers race
// bitfields, and holding the connection lets them come back once it verifies.
// Anything malformed is a protocol violation and drops the connection.
//
// The upload cap is checked after validation and before the disk read, so a
// capped server never buffers bytes it is not about to send.
func (s *Server) serveRequest(ctx context.Context, c *inbound, m wire.Message) error {
	index := int(m.Index)
	if index < 0 || index >= s.pieceCount {
		return fmt.Errorf("%w: request for piece %d, torrent has %d", errDrop, index, s.pieceCount)
	}
	if !s.src.Have(index) {
		return nil
	}
	if m.Length == 0 || m.Length > MaxRequestLength {
		return fmt.Errorf("%w: request length %d outside 1..%d", errDrop, m.Length, MaxRequestLength)
	}
	if int64(m.Begin)+int64(m.Length) > s.meta.PieceSize(index) {
		return fmt.Errorf("%w: request %d+%d overruns piece %d of %d bytes",
			errDrop, m.Begin, m.Length, index, s.meta.PieceSize(index))
	}

	if s.cfg.Limiter != nil {
		if err := s.cfg.Limiter.Wait(ctx, int(m.Length)); err != nil {
			return err
		}
	}
	if s.cfg.ServeDelay > 0 {
		time.Sleep(s.cfg.ServeDelay)
	}

	data, err := s.src.ReadBlock(index, m.Begin, m.Length)
	if err != nil {
		return fmt.Errorf("%w: read block %d+%d of piece %d: %v", errDrop, m.Begin, m.Length, index, err)
	}
	if err := c.write(wire.Message{ID: wire.IDPiece, Index: m.Index, Begin: m.Begin, Block: data}); err != nil {
		return err
	}

	s.uploadedBytes.Add(int64(len(data)))
	s.uploadedBlock.Add(1)
	s.logf("served piece=%d begin=%d length=%d", m.Index, m.Begin, len(data))
	return nil
}

// advertiseHave reports, once per haveInterval, every piece that became held
// after the peer connected. It takes one bitfield snapshot per tick and stops
// when the connection does.
func (s *Server) advertiseHave(ctx context.Context, c *inbound, have []byte, done <-chan struct{}) {
	ticker := time.NewTicker(haveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			held := s.src.HaveBitfield()
			for i := 0; i < s.pieceCount; i++ {
				if wire.BitfieldHas(have, i) || !wire.BitfieldHas(held, i) {
					continue
				}
				wire.BitfieldSet(have, i)
				if err := c.write(wire.Message{ID: wire.IDHave, Index: uint32(i)}); err != nil {
					return
				}
			}
		}
	}
}

// inbound is one connection's I/O. Every frame is written under wmu, so the
// request loop and the have advertiser can share the socket safely.
type inbound struct {
	nc  net.Conn
	r   *bufio.Reader
	wmu sync.Mutex
}

func (c *inbound) write(m wire.Message) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.nc.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	return wire.Write(c.nc, m)
}
