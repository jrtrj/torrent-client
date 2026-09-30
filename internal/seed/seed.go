// Package seed serves verified pieces back to the swarm over the peer wire
// protocol. It is the one upload implementation: cmd/seed (the test fixture)
// and the real client both drive it, so the serving rules live in a single
// place.
//
// Scope is serving, not a general-purpose server. There is no tit-for-tat
// scoring, no rarest-first piece picking, and no rate limiting; the server
// answers requests for exactly the pieces its Source reports as held.
//
// Swarm gate: a Server is built for exactly one torrent, so the handshake is
// checked against that info-hash and nothing else — a per-torrent allow-list,
// not "any torrent we happen to hold". A peer whose handshake names a different
// torrent (or does not speak the protocol) is dropped without a reply. Whether
// we hold the torrent completely is enforced per piece instead: a peer that
// reaches us mid-download is served the pieces already verified, which is what
// makes seeding during the download meaningful.
//
// Choke policy: a fixed set of uploadSlots peers may be unchoked at once. A
// peer is unchoked when it becomes interested and a slot is free; the slot is
// held until the peer loses interest or disconnects, and only unchoked peers'
// requests are answered. The point is to bound state, not to be fair yet:
// uploadSlots caps both how many peers we serve progressively and the transient
// read buffers, and no peer is scored or rotated because the per-peer byte
// accounting a tit-for-tat policy needs is not collected here. A peer that
// arrives while every slot is taken stays choked for the session; slots are not
// rotated, so it would have to reconnect and re-express interest once a slot
// frees. That is a deliberate simplification, not a fairness claim.
//
// Memory: a request is answered by reading the requested span straight from
// the content store at request time (storage.ReadBlock uses pread); no piece
// is ever cached in memory, so a multi-gigabyte torrent costs no more RAM than
// its block buffers. Because only unchoked peers are served and a connection
// handles one request at a time, at most uploadSlots request buffers of at
// most MaxRequestLength bytes are live at once, whatever the torrent size.
//
// Concurrency with the downloader: reads and writes go through pread/pwrite
// (storage.ReadBlock/WritePiece), which share no file offset, so serving a
// piece while another piece is written is safe at the OS level; and a Source
// only reports a piece as held after its verified bytes have been written, so
// the server never reads a piece that is still being assembled.
//
// Per-connection I/O: one goroutine reads requests and writes their replies;
// a second, low-rate goroutine advertises pieces that become held after the
// peer connected, so a peer that meets us mid-download still learns about
// pieces as they verify. Every frame on a connection is written under one
// mutex, so the two writers can never interleave a frame.
package seed

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"torrent-client/internal/metainfo"
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
	// haveInterval is how often a connection looks for pieces that became held
	// after it connected. The download publishes completed pieces to the
	// Source with no event this server can subscribe to, so the server polls;
	// one pass over the piece count per connection per tick is negligible.
	haveInterval = 100 * time.Millisecond
)

// Source is the content side of serving: the pieces we hold and a reader for
// their bytes. The engine's verified-piece set plus the content store satisfy
// it in the real client; a fully-downloaded fixture satisfies it in cmd/seed.
type Source interface {
	// Have reports whether piece index is complete, hash-verified, and so
	// safe to upload. It must be safe to call concurrently.
	Have(index int) bool
	// HaveBitfield returns a bitfield of every piece currently held, as one
	// snapshot. The server takes it once per advertise tick rather than
	// calling Have once per piece, which keeps a many-piece torrent cheap.
	// The returned slice is owned by the caller and may be re-read freely.
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
	return &Server{
		cfg:        cfg,
		meta:       cfg.Meta,
		src:        cfg.Source,
		pieceCount: cfg.Meta.PieceCount(),
		ln:         ln,
		slots:      make(chan struct{}, uploadSlots),
		conns:      make(map[net.Conn]struct{}),
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
	closer := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
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
			if ctx.Err() != nil || s.isClosed() {
				return nil
			}
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.serveConn(ctx, conn)
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
	// Handshake validation is the swarm gate: the info-hash must be the one
	// torrent this server was built for. Anything else is not our swarm and
	// the connection is dropped without a reply. Whether we hold the torrent
	// completely is enforced per piece below, not here, so a peer that
	// reaches us mid-download can still fetch what is already verified.
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
			if err := s.serveRequest(c, m); err != nil {
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

// serveRequest answers one request, or refuses it. A request for a piece we do
// not hold yet is ignored: peers race bitfields, and holding the connection
// lets the peer fetch the piece once it verifies. Anything malformed is a
// protocol violation and drops the connection without reading.
func (s *Server) serveRequest(c *inbound, m wire.Message) error {
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
