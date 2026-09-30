// Command devtracker is the toy tracker the local swarm harness runs against.
//
// A test fixture, not a product: only enough of the announce protocol to pass
// the verification tests, always the compact peer form, and no rate limits or
// peer expiry. HTTP and BEP 15 UDP serve the same swarm, so the harness can
// drive either transport at one tracker.
package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// BEP 15 action ids. Only CONNECT and ANNOUNCE matter here, and the connection
// id is a constant: the fixture doesn't model the protocol's one-minute expiry.
const (
	udpActionConnect  = 0
	udpActionAnnounce = 1
	udpConnectionID   = 0x0123456789abcdef
)

// BEP 15 event codes, which are not the bencoded event names HTTP carries.
const (
	udpEventNone      = 0
	udpEventCompleted = 1
	udpEventStarted   = 2
	udpEventStopped   = 3
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9998", "TCP address to listen on")
	udpAddr := flag.String("udp-addr", "127.0.0.1:0", "UDP address to listen on")
	interval := flag.Int("interval", 5, "announce interval in seconds")
	flag.Parse()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "devtracker: %v\n", err)
		os.Exit(1)
	}
	resolved, err := net.ResolveUDPAddr("udp", *udpAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "devtracker: %v\n", err)
		os.Exit(1)
	}
	conn, err := net.ListenUDP("udp", resolved)
	if err != nil {
		fmt.Fprintf(os.Stderr, "devtracker: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	t := &tracker{
		interval: time.Duration(*interval) * time.Second,
		swarms:   make(map[string]map[string]peer),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/announce", t.announce)

	// Printed so a harness that binds port 0 can learn the ports the kernel
	// picked. The HTTP line comes first so a grep for "listening on" still hits
	// it.
	fmt.Printf("devtracker: listening on %s\n", ln.Addr())
	fmt.Printf("devtracker: udp listening on %s\n", conn.LocalAddr())
	go t.serveUDP(conn)

	if err := http.Serve(ln, mux); err != nil {
		fmt.Fprintf(os.Stderr, "devtracker: %v\n", err)
		os.Exit(1)
	}
}

// peer is one announcing swarm member.
type peer struct {
	ip         net.IP
	port       uint16
	left       int64
	uploaded   int64
	downloaded int64
}

// tracker keeps one swarm per info-hash. Both transports read the same map, so
// an HTTP announce and a UDP announce see each other.
type tracker struct {
	mu       sync.Mutex
	interval time.Duration
	swarms   map[string]map[string]peer // info-hash -> peer id -> peer
}

func (t *tracker) announce(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	infoHash := q.Get("info_hash")
	if len(infoHash) != 20 {
		http.Error(w, "info_hash must be 20 bytes", http.StatusBadRequest)
		return
	}
	peerID := q.Get("peer_id")
	if peerID == "" {
		http.Error(w, "peer_id is required", http.StatusBadRequest)
		return
	}
	port, err := strconv.Atoi(q.Get("port"))
	if err != nil || port < 1 || port > 65535 {
		http.Error(w, "port is required", http.StatusBadRequest)
		return
	}
	left, _ := strconv.ParseInt(q.Get("left"), 10, 64)
	uploaded, _ := strconv.ParseInt(q.Get("uploaded"), 10, 64)
	downloaded, _ := strconv.ParseInt(q.Get("downloaded"), 10, 64)

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	event := q.Get("event")

	t.mu.Lock()
	swarm := t.swarmLocked(infoHash)
	if event == "stopped" {
		delete(swarm, peerID)
	} else {
		swarm[peerID] = peer{
			ip:         net.ParseIP(host),
			port:       uint16(port),
			left:       left,
			uploaded:   uploaded,
			downloaded: downloaded,
		}
	}
	complete, incomplete, compact := t.swarmCompactLocked(swarm, peerID)
	t.mu.Unlock()

	// Log the counters so swarm tests can show a serving client's counters
	// moved. Peer ids are binary, hence %x.
	fmt.Printf("devtracker: announce peer=%x uploaded=%d downloaded=%d left=%d event=%q\n",
		peerID, uploaded, downloaded, left, event)

	w.Header().Set("Content-Type", "text/plain")
	if _, err := w.Write(response(t.interval, complete, incomplete, compact)); err != nil {
		fmt.Fprintf(os.Stderr, "devtracker: write response: %v\n", err)
	}
}

// serveUDP answers BEP 15 datagrams until the socket closes.
func (t *tracker) serveUDP(conn *net.UDPConn) {
	buf := make([]byte, 2048)
	for {
		n, from, err := conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		t.udpPacket(conn, from, buf[:n])
	}
}

// udpPacket answers one CONNECT or ANNOUNCE datagram. Anything else is ignored:
// the fixture speaks only the sliver of the protocol the client actually uses.
func (t *tracker) udpPacket(conn *net.UDPConn, from *net.UDPAddr, req []byte) {
	if len(req) < 16 {
		return // too short to carry an action and a transaction id
	}
	action := binary.BigEndian.Uint32(req[8:12])
	tid := binary.BigEndian.Uint32(req[12:16])

	switch action {
	case udpActionConnect:
		resp := make([]byte, 16)
		binary.BigEndian.PutUint32(resp[0:4], udpActionConnect)
		binary.BigEndian.PutUint32(resp[4:8], tid)
		binary.BigEndian.PutUint64(resp[8:16], udpConnectionID)
		_, _ = conn.WriteToUDP(resp, from)
	case udpActionAnnounce:
		if len(req) < 98 {
			return
		}
		t.udpAnnounce(conn, from, req, tid)
	}
}

func (t *tracker) udpAnnounce(conn *net.UDPConn, from *net.UDPAddr, req []byte, tid uint32) {
	infoHash := string(req[16:36])
	peerID := string(req[36:56])
	downloaded := int64(binary.BigEndian.Uint64(req[56:64]))
	left := int64(binary.BigEndian.Uint64(req[64:72]))
	uploaded := int64(binary.BigEndian.Uint64(req[72:80]))
	event := binary.BigEndian.Uint32(req[80:84])
	port := binary.BigEndian.Uint16(req[96:98])

	t.mu.Lock()
	swarm := t.swarmLocked(infoHash)
	if event == udpEventStopped {
		delete(swarm, peerID)
	} else {
		// The datagram's source address is the peer's address, same as RemoteAddr
		// on the HTTP side.
		swarm[peerID] = peer{ip: from.IP, port: port, left: left, uploaded: uploaded, downloaded: downloaded}
	}
	complete, incomplete, compact := t.swarmCompactLocked(swarm, peerID)
	t.mu.Unlock()

	fmt.Printf("devtracker: announce peer=%x uploaded=%d downloaded=%d left=%d event=%q\n",
		peerID, uploaded, downloaded, left, udpEventName(event))

	resp := make([]byte, 20+len(compact))
	binary.BigEndian.PutUint32(resp[0:4], udpActionAnnounce)
	binary.BigEndian.PutUint32(resp[4:8], tid)
	binary.BigEndian.PutUint32(resp[8:12], uint32(t.interval/time.Second))
	binary.BigEndian.PutUint32(resp[12:16], uint32(incomplete))
	binary.BigEndian.PutUint32(resp[16:20], uint32(complete))
	copy(resp[20:], compact)
	_, _ = conn.WriteToUDP(resp, from)
}

func udpEventName(code uint32) string {
	switch code {
	case udpEventNone:
		return ""
	case udpEventCompleted:
		return "completed"
	case udpEventStarted:
		return "started"
	case udpEventStopped:
		return "stopped"
	}
	return ""
}

func (t *tracker) swarmLocked(infoHash string) map[string]peer {
	swarm := t.swarms[infoHash]
	if swarm == nil {
		swarm = make(map[string]peer)
		t.swarms[infoHash] = swarm
	}
	return swarm
}

// swarmCompactLocked tallies the swarm and renders the compact peer blob. The
// requester is left out — a peer is never handed back to itself.
//
// left=0 is a seeder, anything else a leecher; that's what complete/incomplete
// report, and how a leecher finds the seed.
func (t *tracker) swarmCompactLocked(swarm map[string]peer, requesterID string) (complete, incomplete int, compact []byte) {
	var buf bytes.Buffer
	for id, p := range swarm {
		if p.left == 0 {
			complete++
		} else {
			incomplete++
		}
		if id == requesterID {
			continue
		}
		if v4 := p.ip.To4(); v4 != nil {
			buf.Write(v4)
			buf.Write([]byte{byte(p.port >> 8), byte(p.port)})
		}
	}
	return complete, incomplete, buf.Bytes()
}

// response hand-rolls the bencoded announce reply. Keys go out sorted so strict
// bencode decoders accept it.
func response(interval time.Duration, complete, incomplete int, compactPeers []byte) []byte {
	var b bytes.Buffer
	b.WriteString("d8:completei")
	b.WriteString(strconv.Itoa(complete))
	b.WriteString("e10:incompletei")
	b.WriteString(strconv.Itoa(incomplete))
	b.WriteString("e8:intervali")
	b.WriteString(strconv.Itoa(int(interval / time.Second)))
	b.WriteString("e12:min intervali")
	b.WriteString(strconv.Itoa(int(interval / time.Second / 2)))
	b.WriteString("e5:peers")
	b.WriteString(strconv.Itoa(len(compactPeers)))
	b.WriteString(":")
	b.Write(compactPeers)
	b.WriteString("e")
	return b.Bytes()
}
