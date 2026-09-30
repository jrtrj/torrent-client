package tracker

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

// BEP 15 protocol words. The byte layouts below are fixed-offset big-endian
// binary; there is no bencoding anywhere in this transport.
//
//	CONNECT request  (16 bytes):  0:8 64-bit magic protocol id
//	                              8:12 32-bit action 0
//	                             12:16 32-bit transaction id
//	CONNECT response (16 bytes):  0:4  action 0
//	                              4:8  transaction id
//	                              8:16 64-bit connection id
//	ANNOUNCE request (98 bytes):  0:8   connection id
//	                              8:12  action 1
//	                             12:16  transaction id
//	                             16:36  info-hash (20)
//	                             36:56  peer id (20)
//	                             56:64  downloaded (64-bit)
//	                             64:72  left (64-bit)
//	                             72:80  uploaded (64-bit)
//	                             80:84  event (32-bit)
//	                             84:88  IP address, 0 = "use the source address"
//	                             88:92  key (32-bit)
//	                             92:96  num_want (-1 = tracker default 50)
//	                             96:98  port (16-bit)
//	ANNOUNCE response:            0:4  action 1
//	                              4:8  transaction id
//	                              8:12 interval (seconds)
//	                             12:16 leechers
//	                             16:20 seeders
//	                             20:  peers, 4-byte IPv4 + 2-byte port each
//	SCRAPE request (36+20n):      0:8 connection id, 8:12 action 2,
//	                             12:16 transaction id, then 20-byte hashes
//	SCRAPE response (20+16n):     0:4 action 2, 4:8 transaction id, then a
//	                             32-bit seeders/completed/leechers triple per
//	                             requested hash
//	ERROR response (8+n):         0:4 action 3, 4:8 transaction id, then text
const (
	udpProtocolID = 0x41727101980 // the CONNECT packet's magic "protocol id"

	udpActionConnect  = 0
	udpActionAnnounce = 1
	udpActionScrape   = 2
	udpActionError    = 3
)

// The bencode event spelling used by AnnounceRequest maps onto BEP 15's fixed
// numeric codes. Zero is "no event", which is what a periodic re-announce and
// the first announce (the engine always sends EventStarted) would otherwise
// leave behind.
const (
	udpEventNone      = 0
	udpEventCompleted = 1
	udpEventStarted   = 2
	udpEventStopped   = 3
)

// Packet lengths, byte-exact. The request lengths are what we emit; the
// response minimums are what we refuse to interpret anything shorter than.
const (
	udpConnectRequestLen  = 16
	udpConnectResponseLen = 16
	udpAnnounceRequestLen = 98
	udpAnnounceHeaderLen  = 20
	udpScrapeRequestLen   = 36
	udpScrapeResponseLen  = 20
	udpErrorHeaderLen     = 8
)

// connectionIDTTL is how long a connection id is reused before we pay for a
// fresh CONNECT. BEP 15 only promises validity for about a minute, so we
// refresh on that boundary instead of risking an announce the tracker drops
// as stale.
const connectionIDTTL = time.Minute

// udpRetry bounds one request/response exchange. BEP 15's own backoff starts
// at 15s and doubles to 384s over 8 attempts (~8 minutes), which is far too
// long for a client to sit on one dead tracker: the engine re-announces on
// its own schedule and would rather fail fast and try the next announce URL.
// We keep the doubling shape but start lower and cap every wait at 15s over 5
// attempts, so one request is bounded at 2+4+8+15+15 = 44s.
type udpRetry struct {
	Attempts int
	Base     time.Duration
	Max      time.Duration
}

// timeout is the wait before the given zero-based attempt is considered lost.
func (p udpRetry) timeout(attempt int) time.Duration {
	d := p.Base
	for i := 0; i < attempt && d < p.Max; i++ {
		d *= 2
	}
	if d > p.Max {
		d = p.Max
	}
	return d
}

// defaultUDPRetry is the capped policy described on udpRetry: 44s worst case
// for a single request, against BEP 15's ~8 minutes.
var defaultUDPRetry = udpRetry{Attempts: 5, Base: 2 * time.Second, Max: 15 * time.Second}

// Scrape is one info-hash's swarm counters from a BEP 15 scrape.
type Scrape struct {
	Complete   int // seeders
	Downloaded int // completed downloads
	Incomplete int // leechers
}

// UDPTracker announces with the BEP 15 UDP protocol.
//
// Socket shape: one connected socket per tracker, dialled lazily and kept for
// the tracker's whole life. A tracker identifies a peer by its source address
// and port, so re-dialling from a fresh ephemeral port between announces would
// look like a different peer; a stable source port keeps every announce in the
// same slot. A connected UDP socket also makes the kernel drop datagrams from
// anyone but the dialled tracker address, which is a second layer under the
// transaction-id check in exchange.
type UDPTracker struct {
	URL string

	addrString string
	retry      udpRetry
	key        uint32 // stable announce key, identifies us if our IP changes

	mu      sync.Mutex
	conn    *net.UDPConn
	connID  uint64 // cached connection id; 0 means "none", never issued
	connExp time.Time
}

// NewUDP returns a BEP 15 transport for one udp:// announce URL. The tracker is
// contacted lazily, so a name that does not resolve yet is not fatal here.
func NewUDP(announceURL string) (*UDPTracker, error) {
	u, err := url.Parse(announceURL)
	if err != nil {
		return nil, fmt.Errorf("udp tracker URL %q: %w", announceURL, err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("udp tracker URL %q has no host:port", announceURL)
	}
	if u.Port() == "" {
		return nil, fmt.Errorf("udp tracker URL %q has no port", announceURL)
	}
	key, err := randomUint32()
	if err != nil {
		return nil, fmt.Errorf("udp tracker %q: %w", announceURL, err)
	}
	return &UDPTracker{URL: announceURL, addrString: u.Host, retry: defaultUDPRetry, key: key}, nil
}

// Close releases the socket. Announce is unusable afterwards; the CLI calls
// this when it moves on to the next announce URL.
func (t *UDPTracker) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closeLocked()
}

func (t *UDPTracker) closeLocked() error {
	if t.conn == nil {
		return nil
	}
	err := t.conn.Close()
	t.conn = nil
	t.connID = 0
	t.connExp = time.Time{}
	return err
}

// Announce runs the CONNECT-then-ANNOUNCE dance and decodes the reply.
func (t *UDPTracker) Announce(ctx context.Context, req AnnounceRequest) (AnnounceResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	conn, err := t.socket(ctx)
	if err != nil {
		return AnnounceResponse{}, err
	}
	connID, err := t.connectionID(ctx, conn)
	if err != nil {
		return AnnounceResponse{}, err
	}
	tid, err := randomUint32()
	if err != nil {
		return AnnounceResponse{}, err
	}

	packet := announcePacket(connID, req, t.key)
	binary.BigEndian.PutUint32(packet[12:16], tid)
	resp, err := t.exchange(ctx, conn, packet, udpActionAnnounce)
	if err != nil {
		return AnnounceResponse{}, err
	}
	return parseUDPAnnounce(resp, t.URL)
}

// Scrape asks for one info-hash's swarm counters. It is the third leg of the
// BEP 15 flow; nothing in the client needs it, but the transport is the only
// place that can speak it, so it is exposed here rather than re-derived later.
func (t *UDPTracker) Scrape(ctx context.Context, infoHash [20]byte) (Scrape, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	conn, err := t.socket(ctx)
	if err != nil {
		return Scrape{}, err
	}
	connID, err := t.connectionID(ctx, conn)
	if err != nil {
		return Scrape{}, err
	}
	tid, err := randomUint32()
	if err != nil {
		return Scrape{}, err
	}

	packet := make([]byte, udpScrapeRequestLen)
	binary.BigEndian.PutUint64(packet[0:8], connID)
	binary.BigEndian.PutUint32(packet[8:12], udpActionScrape)
	binary.BigEndian.PutUint32(packet[12:16], tid)
	copy(packet[16:36], infoHash[:])

	resp, err := t.exchange(ctx, conn, packet, udpActionScrape)
	if err != nil {
		return Scrape{}, err
	}
	if len(resp) < udpScrapeResponseLen {
		return Scrape{}, &Error{Transient: true, Err: fmt.Errorf(
			"udp tracker %s: scrape reply is %d bytes, want at least %d", t.URL, len(resp), udpScrapeResponseLen)}
	}
	return Scrape{
		Complete:   int(binary.BigEndian.Uint32(resp[8:12])),
		Downloaded: int(binary.BigEndian.Uint32(resp[12:16])),
		Incomplete: int(binary.BigEndian.Uint32(resp[16:20])),
	}, nil
}

// socket returns the cached connected socket, dialling it on first use.
// Callers hold t.mu.
func (t *UDPTracker) socket(ctx context.Context) (*net.UDPConn, error) {
	if t.conn != nil {
		return t.conn, nil
	}
	var d net.Dialer
	c, err := d.DialContext(ctx, "udp", t.addrString)
	if err != nil {
		return nil, &Error{Transient: true, Err: fmt.Errorf("udp tracker %s: %w", t.URL, err)}
	}
	uc, ok := c.(*net.UDPConn)
	if !ok {
		_ = c.Close()
		return nil, &Error{Transient: false, Err: fmt.Errorf("udp tracker %s: dial returned %T, want *net.UDPConn", t.URL, c)}
	}
	t.conn = uc
	return t.conn, nil
}

// connectionID returns a cached connection id, running CONNECT when none is
// live. Callers hold t.mu.
func (t *UDPTracker) connectionID(ctx context.Context, conn *net.UDPConn) (uint64, error) {
	if t.connID != 0 && time.Now().Before(t.connExp) {
		return t.connID, nil
	}
	tid, err := randomUint32()
	if err != nil {
		return 0, err
	}
	resp, err := t.exchange(ctx, conn, connectPacket(tid), udpActionConnect)
	if err != nil {
		return 0, err
	}
	if len(resp) < udpConnectResponseLen {
		return 0, &Error{Transient: true, Err: fmt.Errorf(
			"udp tracker %s: connect reply is %d bytes, want %d", t.URL, len(resp), udpConnectResponseLen)}
	}
	t.connID = binary.BigEndian.Uint64(resp[8:16])
	t.connExp = time.Now().Add(connectionIDTTL)
	return t.connID, nil
}

// invalidate forgets the cached connection id, so the next attempt re-CONNECTs.
// Callers hold t.mu.
func (t *UDPTracker) invalidate() {
	t.connID = 0
	t.connExp = time.Time{}
}

// exchange sends req and returns the first reply that carries our transaction
// id and the action we expect. It retransmits on timeout with the capped
// backoff, and deliberately ignores anything else that arrives: a datagram
// with a foreign transaction id is either a stale reply or a spoof, and acting
// on it would let anyone who can reach our port steer the swarm. Runt or
// malformed datagrams are dropped the same way rather than parsed.
//
// Deadlines are set per packet, right before each write and read, so a slow
// exchange never shortens the window available to the next one.
func (t *UDPTracker) exchange(ctx context.Context, conn *net.UDPConn, req []byte, action uint32) ([]byte, error) {
	tid := binary.BigEndian.Uint32(req[12:16])

	// Unblock a pending read the instant the context is cancelled, so Ctrl-C
	// does not have to wait out the per-packet deadline.
	stop := context.AfterFunc(ctx, func() { _ = conn.SetReadDeadline(time.Now()) })
	defer stop()

	buf := make([]byte, 65536)
	var lastErr error
	for attempt := 0; attempt < t.retry.Attempts; attempt++ {
		if err := ctxErr(ctx); err != nil {
			return nil, err
		}
		deadline := time.Now().Add(t.retry.timeout(attempt))
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}

		if err := conn.SetWriteDeadline(deadline); err != nil {
			return nil, t.transient(err)
		}
		if _, err := conn.Write(req); err != nil {
			if e := ctxErr(ctx); e != nil {
				return nil, e
			}
			lastErr = err
			continue
		}

		for {
			if err := conn.SetReadDeadline(deadline); err != nil {
				return nil, t.transient(err)
			}
			n, err := conn.Read(buf)
			if err != nil {
				var nerr net.Error
				if errors.As(err, &nerr) && nerr.Timeout() {
					// A context deadline that has just elapsed looks like a
					// socket timeout; report it as the cancellation it is.
					if e := ctxErr(ctx); e != nil {
						return nil, e
					}
					lastErr = err
					break // retransmit with a longer wait
				}
				if e := ctxErr(ctx); e != nil {
					return nil, e
				}
				return nil, t.transient(err)
			}
			resp := buf[:n]
			if len(resp) < udpErrorHeaderLen {
				lastErr = fmt.Errorf("short datagram of %d bytes", len(resp))
				continue
			}
			if got := binary.BigEndian.Uint32(resp[4:8]); got != tid {
				lastErr = fmt.Errorf("datagram with transaction id %d, want %d", got, tid)
				continue
			}
			switch got := binary.BigEndian.Uint32(resp[0:4]); got {
			case udpActionError:
				// The frame may report a stale connection id, so make the
				// next attempt buy a fresh one instead of replaying ours.
				t.invalidate()
				return nil, errorFrame(t.URL, resp)
			case action:
				return resp, nil
			default:
				lastErr = fmt.Errorf("action %d, want %d", got, action)
				continue
			}
		}
	}
	return nil, &Error{Transient: true, Err: fmt.Errorf(
		"udp tracker %s: no reply after %d attempts: %w", t.URL, t.retry.Attempts, lastErr)}
}

func (t *UDPTracker) transient(err error) error {
	return &Error{Transient: true, Err: fmt.Errorf("udp tracker %s: %w", t.URL, err)}
}

// ctxErr reports the context's cancellation, including a deadline that has
// passed but whose timer has not yet been observed. The socket's own timeout
// is clamped to the context deadline, so the two fire together and this
// resolves that tie in favour of the real cause.
func ctxErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d, ok := ctx.Deadline(); ok && !time.Now().Before(d) {
		return context.DeadlineExceeded
	}
	return nil
}

// connectPacket builds the CONNECT request: the magic protocol id, action 0,
// and our transaction id.
func connectPacket(tid uint32) []byte {
	b := make([]byte, udpConnectRequestLen)
	binary.BigEndian.PutUint64(b[0:8], udpProtocolID)
	binary.BigEndian.PutUint32(b[8:12], udpActionConnect)
	binary.BigEndian.PutUint32(b[12:16], tid)
	return b
}

// announcePacket lays AnnounceRequest into the fixed 98-byte announce frame.
// The IP field is left 0 so the tracker uses the datagram's source address,
// and num_want is -1 (the protocol's "tracker picks, default 50") unless the
// caller asked for a specific count.
func announcePacket(connID uint64, req AnnounceRequest, key uint32) []byte {
	b := make([]byte, udpAnnounceRequestLen)
	binary.BigEndian.PutUint64(b[0:8], connID)
	binary.BigEndian.PutUint32(b[8:12], udpActionAnnounce)
	copy(b[16:36], req.InfoHash[:])
	copy(b[36:56], req.PeerID[:])
	binary.BigEndian.PutUint64(b[56:64], uint64(req.Downloaded))
	binary.BigEndian.PutUint64(b[64:72], uint64(req.Left))
	binary.BigEndian.PutUint64(b[72:80], uint64(req.Uploaded))
	binary.BigEndian.PutUint32(b[80:84], udpEventCode(req.Event))

	numWant := int32(-1)
	if req.NumWant > 0 {
		numWant = int32(req.NumWant)
	}
	binary.BigEndian.PutUint32(b[92:96], uint32(numWant))
	binary.BigEndian.PutUint16(b[96:98], req.Port)
	return b
}

func udpEventCode(ev Event) uint32 {
	switch ev {
	case EventCompleted:
		return udpEventCompleted
	case EventStarted:
		return udpEventStarted
	case EventStopped:
		return udpEventStopped
	default:
		return udpEventNone
	}
}

// parseUDPAnnounce decodes the announce reply's header and its compact peer
// list. The 6-byte peer chunks are byte-identical to the HTTP compact form, so
// the port-0 filter and address handling match that transport. BEP 15 carries
// no IPv6 peers (there is no peers6 counterpart), so an IPv4-only swarm is the
// honest limit here.
func parseUDPAnnounce(resp []byte, url string) (AnnounceResponse, error) {
	if len(resp) < udpAnnounceHeaderLen {
		return AnnounceResponse{}, &Error{Transient: true, Err: fmt.Errorf(
			"udp tracker %s: announce reply is %d bytes, want at least %d", url, len(resp), udpAnnounceHeaderLen)}
	}
	body := resp[udpAnnounceHeaderLen:]
	if len(body)%6 != 0 {
		return AnnounceResponse{}, &Error{Transient: true, Err: fmt.Errorf(
			"udp tracker %s: %d peer bytes is not a multiple of 6", url, len(body))}
	}
	ar := AnnounceResponse{
		Interval:   time.Duration(binary.BigEndian.Uint32(resp[8:12])) * time.Second,
		Incomplete: int(binary.BigEndian.Uint32(resp[12:16])),
		Complete:   int(binary.BigEndian.Uint32(resp[16:20])),
	}
	for i := 0; i+6 <= len(body); i += 6 {
		port := binary.BigEndian.Uint16(body[i+4 : i+6])
		if port == 0 {
			continue
		}
		ar.Peers = append(ar.Peers, Peer{
			IP:   net.IPv4(body[i], body[i+1], body[i+2], body[i+3]),
			Port: port,
		})
	}
	return ar, nil
}

// errorFrame turns an action=3 datagram's trailing text into a tracker error.
// The text cannot be classified as permanent or transient reliably ("connection
// id expired" and "torrent not registered" arrive the same way), so it is
// reported as transient: the retry re-CONNECTs and the engine's own retry and
// re-announce schedule decide when to give up.
func errorFrame(url string, resp []byte) error {
	msg := strings.TrimSpace(string(resp[udpErrorHeaderLen:]))
	if msg == "" {
		msg = "tracker returned an empty error frame"
	}
	return &Error{Transient: true, Err: fmt.Errorf("udp tracker %s refused the announce: %s", url, msg)}
}

// randomUint32 draws a transaction id (or an announce key) from the system
// CSPRNG. Transaction ids must be unpredictable: matching replies on them is
// the only thing standing between us and a forged announce response.
func randomUint32() (uint32, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b[:]), nil
}
