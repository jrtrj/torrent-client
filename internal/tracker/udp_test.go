package tracker

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// udpStub is a minimal in-process BEP 15 tracker. It owns a real UDP socket,
// decodes the fixed-offset requests, and answers from whatever the running test
// configured. Tests replace handler to script spoofs, runts, and error frames.
type udpStub struct {
	t    *testing.T
	conn *net.UDPConn

	// Scripted reply state.
	connID     uint64
	interval   uint32
	complete   uint32
	incomplete uint32
	peers      []byte
	errMsg     string
	// handler, when set, replaces the default handling entirely. send may be
	// called any number of times, which is how a test emits a spoof followed
	// by the genuine reply.
	handler func(req []byte, send func([]byte))

	mu           sync.Mutex
	once         sync.Once
	connects     int
	announces    int
	lastAnnounce []byte
}

func newUDPStub(t *testing.T) *udpStub {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	s := &udpStub{
		t:        t,
		conn:     conn,
		connID:   0x1122334455667788,
		interval: 1800,
		complete: 3,
	}
	t.Cleanup(func() { _ = conn.Close() })
	return s
}

// url starts the reply loop the first time the address is needed. Starting the
// goroutine only after the caller has finished configuring the stub gives the
// loop a happens-before edge over that configuration, so the test writes and
// the loop reads never race.
func (s *udpStub) url() string {
	s.once.Do(func() { go s.loop() })
	return "udp://" + s.conn.LocalAddr().String() + "/announce"
}

func (s *udpStub) loop() {
	buf := make([]byte, 2048)
	for {
		n, from, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			return // socket closed by the test cleanup
		}
		req := append([]byte(nil), buf[:n]...)
		if len(req) < 16 {
			continue
		}
		send := func(reply []byte) {
			if reply != nil {
				_, _ = s.conn.WriteToUDP(reply, from)
			}
		}
		if s.handler != nil {
			s.handler(req, send)
			continue
		}
		s.defaultHandler(req, send)
	}
}

// defaultHandler answers CONNECT, ANNOUNCE, and an ERROR frame when errMsg is
// set.
func (s *udpStub) defaultHandler(req []byte, send func([]byte)) {
	action := binary.BigEndian.Uint32(req[8:12])
	tid := binary.BigEndian.Uint32(req[12:16])
	switch action {
	case udpActionConnect:
		s.mu.Lock()
		s.connects++
		s.mu.Unlock()
		send(s.connectReply(tid))
	case udpActionAnnounce:
		s.mu.Lock()
		s.announces++
		s.lastAnnounce = append([]byte(nil), req...)
		errMsg := s.errMsg
		s.mu.Unlock()
		if errMsg != "" {
			send(errorReply(tid, errMsg))
			return
		}
		send(s.announceReply(tid))
	case udpActionScrape:
		send(s.scrapeReply(tid))
	}
}

func (s *udpStub) connectReply(tid uint32) []byte {
	resp := make([]byte, udpConnectResponseLen)
	binary.BigEndian.PutUint32(resp[0:4], udpActionConnect)
	binary.BigEndian.PutUint32(resp[4:8], tid)
	binary.BigEndian.PutUint64(resp[8:16], s.connID)
	return resp
}

func (s *udpStub) announceReply(tid uint32) []byte {
	s.mu.Lock()
	peers := append([]byte(nil), s.peers...)
	interval, complete, incomplete := s.interval, s.complete, s.incomplete
	s.mu.Unlock()

	resp := make([]byte, udpAnnounceHeaderLen+len(peers))
	binary.BigEndian.PutUint32(resp[0:4], udpActionAnnounce)
	binary.BigEndian.PutUint32(resp[4:8], tid)
	binary.BigEndian.PutUint32(resp[8:12], interval)
	binary.BigEndian.PutUint32(resp[12:16], incomplete)
	binary.BigEndian.PutUint32(resp[16:20], complete)
	copy(resp[udpAnnounceHeaderLen:], peers)
	return resp
}

func (s *udpStub) scrapeReply(tid uint32) []byte {
	resp := make([]byte, udpScrapeResponseLen)
	binary.BigEndian.PutUint32(resp[0:4], udpActionScrape)
	binary.BigEndian.PutUint32(resp[4:8], tid)
	binary.BigEndian.PutUint32(resp[8:12], 7)  // seeders
	binary.BigEndian.PutUint32(resp[12:16], 9) // completed
	binary.BigEndian.PutUint32(resp[16:20], 2)
	return resp
}

func errorReply(tid uint32, msg string) []byte {
	resp := make([]byte, udpErrorHeaderLen+len(msg))
	binary.BigEndian.PutUint32(resp[0:4], udpActionError)
	binary.BigEndian.PutUint32(resp[4:8], tid)
	copy(resp[udpErrorHeaderLen:], msg)
	return resp
}

// compactPeer renders one 6-byte IPv4 peer entry.
func compactPeer(ip string, port uint16) []byte {
	b := make([]byte, 6)
	copy(b, net.ParseIP(ip).To4())
	binary.BigEndian.PutUint16(b[4:6], port)
	return b
}

func (s *udpStub) counts() (connects, announces int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connects, s.announces
}

// fastRetry makes a transport fail quickly so tests that exercise the give-up
// paths do not wait out the production backoff.
func fastRetry(tr *UDPTracker) {
	tr.retry = udpRetry{Attempts: 3, Base: 5 * time.Millisecond, Max: 10 * time.Millisecond}
}

func TestUDPAnnounceConnectsThenAnnouncesAndDecodesPeers(t *testing.T) {
	s := newUDPStub(t)
	// 127.0.0.1:8080 is real; 10.0.0.7:0 must be dropped exactly as the HTTP
	// transport drops port-0 peers.
	s.peers = append(compactPeer("127.0.0.1", 8080), compactPeer("10.0.0.7", 0)...)

	tr, err := NewUDP(s.url())
	if err != nil {
		t.Fatalf("NewUDP: %v", err)
	}
	defer tr.Close()

	req := testRequest()
	req.Event = EventStarted
	req.Uploaded = 77
	resp, err := tr.Announce(context.Background(), req)
	if err != nil {
		t.Fatalf("Announce: %v", err)
	}

	if len(resp.Peers) != 1 || resp.Peers[0].Addr() != "127.0.0.1:8080" {
		t.Fatalf("peers = %v, want 127.0.0.1:8080", resp.Peers)
	}
	if resp.Interval != 1800*time.Second {
		t.Fatalf("interval = %s, want 30m", resp.Interval)
	}
	if resp.Complete != 3 {
		t.Fatalf("complete = %d, want 3", resp.Complete)
	}

	connects, announces := s.counts()
	if connects != 1 || announces != 1 {
		t.Fatalf("connects/announces = %d/%d, want 1/1", connects, announces)
	}

	// The announce frame must carry the request's fields at their fixed
	// offsets, and the CONNECT magic must have come first.
	s.mu.Lock()
	pkt := append([]byte(nil), s.lastAnnounce...)
	s.mu.Unlock()
	if len(pkt) != udpAnnounceRequestLen {
		t.Fatalf("announce packet is %d bytes, want %d", len(pkt), udpAnnounceRequestLen)
	}
	if binary.BigEndian.Uint64(pkt[0:8]) != s.connID {
		t.Fatalf("announce connection id = %#x, want %#x", binary.BigEndian.Uint64(pkt[0:8]), s.connID)
	}
	if binary.BigEndian.Uint32(pkt[8:12]) != udpActionAnnounce {
		t.Fatalf("announce action = %d, want %d", binary.BigEndian.Uint32(pkt[8:12]), udpActionAnnounce)
	}
	if string(pkt[16:36]) != string(req.InfoHash[:]) {
		t.Fatalf("info_hash = %x, want %x", pkt[16:36], req.InfoHash)
	}
	if string(pkt[36:56]) != string(req.PeerID[:]) {
		t.Fatalf("peer_id = %x, want %x", pkt[36:56], req.PeerID)
	}
	if got := binary.BigEndian.Uint64(pkt[56:64]); got != uint64(req.Downloaded) {
		t.Fatalf("downloaded = %d, want %d", got, req.Downloaded)
	}
	if got := binary.BigEndian.Uint64(pkt[64:72]); got != uint64(req.Left) {
		t.Fatalf("left = %d, want %d", got, req.Left)
	}
	if got := binary.BigEndian.Uint64(pkt[72:80]); got != 77 {
		t.Fatalf("uploaded = %d, want 77", got)
	}
	if got := binary.BigEndian.Uint32(pkt[80:84]); got != udpEventStarted {
		t.Fatalf("event = %d, want %d (started)", got, udpEventStarted)
	}
	if got := binary.BigEndian.Uint32(pkt[92:96]); got != uint32(int32(req.NumWant)) {
		t.Fatalf("num_want = %d, want %d", int32(got), req.NumWant)
	}
	if got := binary.BigEndian.Uint16(pkt[96:98]); got != req.Port {
		t.Fatalf("port = %d, want %d", got, req.Port)
	}
}

// A periodic re-announce carries no event; a stopped announce carries code 3.
func TestUDPMapsEventCodes(t *testing.T) {
	for _, tc := range []struct {
		event Event
		want  uint32
	}{
		{"", udpEventNone},
		{EventStarted, udpEventStarted},
		{EventCompleted, udpEventCompleted},
		{EventStopped, udpEventStopped},
	} {
		if got := udpEventCode(tc.event); got != tc.want {
			t.Fatalf("udpEventCode(%q) = %d, want %d", tc.event, got, tc.want)
		}
	}
}

func TestUDPReusesConnectionIDWithinTTL(t *testing.T) {
	s := newUDPStub(t)
	tr, err := NewUDP(s.url())
	if err != nil {
		t.Fatalf("NewUDP: %v", err)
	}
	defer tr.Close()

	for i := 0; i < 3; i++ {
		if _, err := tr.Announce(context.Background(), testRequest()); err != nil {
			t.Fatalf("announce %d: %v", i, err)
		}
	}
	connects, announces := s.counts()
	if connects != 1 {
		t.Fatalf("connects = %d, want 1 (the id must be reused within its TTL)", connects)
	}
	if announces != 3 {
		t.Fatalf("announces = %d, want 3", announces)
	}
}

// A connection id past its one-minute window must force a fresh CONNECT.
func TestUDPReconnectsWhenConnectionIDIsStale(t *testing.T) {
	s := newUDPStub(t)
	tr, err := NewUDP(s.url())
	if err != nil {
		t.Fatalf("NewUDP: %v", err)
	}
	defer tr.Close()

	if _, err := tr.Announce(context.Background(), testRequest()); err != nil {
		t.Fatalf("first announce: %v", err)
	}
	tr.mu.Lock()
	tr.connExp = time.Now().Add(-time.Second) // simulate the TTL lapsing
	tr.mu.Unlock()

	if _, err := tr.Announce(context.Background(), testRequest()); err != nil {
		t.Fatalf("second announce: %v", err)
	}
	connects, announces := s.counts()
	if connects != 2 {
		t.Fatalf("connects = %d, want 2 (a stale id must trigger a reconnect)", connects)
	}
	if announces != 2 {
		t.Fatalf("announces = %d, want 2", announces)
	}
}

// A tracker that answers an announce with "connection id expired" must make
// the client forget the cached id, so the next announce re-CONNECTs. This is
// the tracker-side route to the same reconnect.
func TestUDPErrorFrameInvalidatesConnectionID(t *testing.T) {
	s := newUDPStub(t)
	s.mu.Lock()
	s.errMsg = "Connection ID expired"
	s.mu.Unlock()

	tr, err := NewUDP(s.url())
	if err != nil {
		t.Fatalf("NewUDP: %v", err)
	}
	defer tr.Close()

	_, err = tr.Announce(context.Background(), testRequest())
	if err == nil {
		t.Fatal("an error frame did not surface as an error")
	}
	if !strings.Contains(err.Error(), "Connection ID expired") {
		t.Fatalf("err = %v, want it to carry the tracker's text", err)
	}
	tr.mu.Lock()
	cached := tr.connID
	tr.mu.Unlock()
	if cached != 0 {
		t.Fatalf("cached connection id = %#x, want it dropped", cached)
	}

	// With the id forgotten, the next announce must CONNECT again.
	s.mu.Lock()
	s.errMsg = ""
	s.mu.Unlock()
	if _, err := tr.Announce(context.Background(), testRequest()); err != nil {
		t.Fatalf("announce after the error frame: %v", err)
	}
	if connects, _ := s.counts(); connects != 2 {
		t.Fatalf("connects = %d, want 2", connects)
	}
}

// Any datagram whose transaction id is not ours is a spoof or a stale reply:
// it must be ignored, and the genuine reply that follows must win.
func TestUDPIgnoresSpoofedTransactionID(t *testing.T) {
	s := newUDPStub(t)
	spoiled := compactPeer("1.2.3.4", 9999)
	s.peers = compactPeer("127.0.0.1", 6881)

	s.handler = func(req []byte, send func([]byte)) {
		if binary.BigEndian.Uint32(req[8:12]) != udpActionAnnounce {
			s.defaultHandler(req, send)
			return
		}
		tid := binary.BigEndian.Uint32(req[12:16])
		// A forged announce reply with a foreign transaction id and an
		// attacker-chosen peer, delivered before the real one.
		spoof := make([]byte, udpAnnounceHeaderLen+len(spoiled))
		binary.BigEndian.PutUint32(spoof[0:4], udpActionAnnounce)
		binary.BigEndian.PutUint32(spoof[4:8], tid^0xdeadbeef)
		binary.BigEndian.PutUint32(spoof[8:12], 1)
		copy(spoof[udpAnnounceHeaderLen:], spoiled)
		send(spoof)
		send(s.announceReply(tid))
	}

	tr, err := NewUDP(s.url())
	if err != nil {
		t.Fatalf("NewUDP: %v", err)
	}
	defer tr.Close()

	resp, err := tr.Announce(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("Announce: %v", err)
	}
	if len(resp.Peers) != 1 || resp.Peers[0].Addr() != "127.0.0.1:6881" {
		t.Fatalf("peers = %v, want the genuine 127.0.0.1:6881 (the spoof must be dropped)", resp.Peers)
	}
	if resp.Interval != 1800*time.Second {
		t.Fatalf("interval = %s, want 30m from the genuine reply", resp.Interval)
	}
}

// Runts and unparseable datagrams must be dropped, not parsed and not fatal:
// the real reply that follows still lands.
func TestUDPDropsMalformedDatagramsAndKeepsGoing(t *testing.T) {
	s := newUDPStub(t)
	s.peers = compactPeer("127.0.0.1", 6881)

	s.handler = func(req []byte, send func([]byte)) {
		if binary.BigEndian.Uint32(req[8:12]) != udpActionAnnounce {
			s.defaultHandler(req, send)
			return
		}
		send([]byte{1, 2, 3})                               // a runt, shorter than any header
		send([]byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 1}) // unknown/foreign frame
		s.defaultHandler(req, send)
	}

	tr, err := NewUDP(s.url())
	if err != nil {
		t.Fatalf("NewUDP: %v", err)
	}
	defer tr.Close()

	resp, err := tr.Announce(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("Announce after malformed datagrams: %v", err)
	}
	if len(resp.Peers) != 1 || resp.Peers[0].Addr() != "127.0.0.1:6881" {
		t.Fatalf("peers = %v, want 127.0.0.1:6881", resp.Peers)
	}
}

// With nothing but garbage on the wire the client gives up with a transient
// error instead of panicking or hanging forever.
func TestUDPPermanentGarbageFailsTransientlyWithoutPanic(t *testing.T) {
	s := newUDPStub(t)
	s.handler = func(_ []byte, send func([]byte)) {
		send([]byte{0, 1, 2}) // never a valid frame
	}

	tr, err := NewUDP(s.url())
	if err != nil {
		t.Fatalf("NewUDP: %v", err)
	}
	defer tr.Close()
	fastRetry(tr)

	_, err = tr.Announce(context.Background(), testRequest())
	if err == nil {
		t.Fatal("garbage was accepted as a reply")
	}
	if IsFatal(err) {
		t.Fatalf("err = %v, want a transient error", err)
	}
}

// A datagram that succeeds the request but its reply is lost: the client must
// retransmit and accept the answer to the retry.
func TestUDPRetransmitsOnTimeout(t *testing.T) {
	s := newUDPStub(t)
	s.peers = compactPeer("127.0.0.1", 6881)

	var seen int
	s.handler = func(req []byte, send func([]byte)) {
		if binary.BigEndian.Uint32(req[8:12]) != udpActionAnnounce {
			s.defaultHandler(req, send)
			return
		}
		s.mu.Lock()
		seen++
		n := seen
		s.mu.Unlock()
		if n == 1 {
			return // drop the first announce, as a lost datagram would be
		}
		s.defaultHandler(req, send)
	}

	tr, err := NewUDP(s.url())
	if err != nil {
		t.Fatalf("NewUDP: %v", err)
	}
	defer tr.Close()
	fastRetry(tr)

	resp, err := tr.Announce(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("Announce: %v", err)
	}
	if len(resp.Peers) != 1 || resp.Peers[0].Addr() != "127.0.0.1:6881" {
		t.Fatalf("peers = %v, want 127.0.0.1:6881", resp.Peers)
	}
	s.mu.Lock()
	attempts := seen
	s.mu.Unlock()
	if attempts < 2 {
		t.Fatalf("announce datagrams seen = %d, want the client to have retransmitted", attempts)
	}
}

func TestUDPErrorFrameSurfacesTrackerMessage(t *testing.T) {
	s := newUDPStub(t)
	s.mu.Lock()
	s.errMsg = "torrent not registered with this tracker"
	s.mu.Unlock()

	tr, err := NewUDP(s.url())
	if err != nil {
		t.Fatalf("NewUDP: %v", err)
	}
	defer tr.Close()

	_, err = tr.Announce(context.Background(), testRequest())
	if err == nil {
		t.Fatal("an action=3 error frame was not surfaced")
	}
	if !strings.Contains(err.Error(), "torrent not registered with this tracker") {
		t.Fatalf("err = %v, want the tracker's text", err)
	}
}

func TestUDPScrapeReportsSwarmCounters(t *testing.T) {
	s := newUDPStub(t)
	tr, err := NewUDP(s.url())
	if err != nil {
		t.Fatalf("NewUDP: %v", err)
	}
	defer tr.Close()

	var infoHash [20]byte
	copy(infoHash[:], "01234567890123456789")
	got, err := tr.Scrape(context.Background(), infoHash)
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	if got.Complete != 7 || got.Downloaded != 9 || got.Incomplete != 2 {
		t.Fatalf("scrape = %+v, want complete 7, downloaded 9, incomplete 2", got)
	}
}

// The production backoff must stay bounded: BEP 15 allows ~8 minutes, which is
// far too long for this client, so the cap over the whole request is the
// contract being pinned here.
func TestUDPBackoffIsBoundedByTheCap(t *testing.T) {
	const cap = 15 * time.Second
	p := defaultUDPRetry

	if p.Attempts != 5 {
		t.Fatalf("attempts = %d, want 5", p.Attempts)
	}
	if p.Max > cap {
		t.Fatalf("per-attempt cap = %s, want at most %s", p.Max, cap)
	}
	prev := time.Duration(0)
	total := time.Duration(0)
	for i := 0; i < p.Attempts; i++ {
		d := p.timeout(i)
		if d > p.Max {
			t.Fatalf("timeout(%d) = %s, exceeds the cap %s", i, d, p.Max)
		}
		if d < prev {
			t.Fatalf("timeout(%d) = %s shrank from %s", i, d, prev)
		}
		prev = d
		total += d
	}
	if total > 44*time.Second {
		t.Fatalf("total wait = %s, want at most 44s", total)
	}
	if got := p.timeout(p.Attempts - 1); got != p.Max {
		t.Fatalf("last timeout = %s, want the cap %s", got, p.Max)
	}
}

// The context must cut a stalled exchange short rather than let it run out the
// backoff.
func TestUDPAnnounceHonoursContextCancellation(t *testing.T) {
	s := newUDPStub(t)
	s.handler = func(_ []byte, _ func([]byte)) {} // reply to nothing

	tr, err := NewUDP(s.url())
	if err != nil {
		t.Fatalf("NewUDP: %v", err)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = tr.Announce(ctx, testRequest())
	if err == nil {
		t.Fatal("a cancelled announce reported success")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("cancellation took %s, want it prompt", elapsed)
	}
}
