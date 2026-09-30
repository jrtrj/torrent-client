// Command devtracker is the in-repo development tracker used by the local
// swarm harness.
//
// It is a test fixture, not a product: it implements only enough of the
// announce protocol to serve the verification tests, and nothing it does
// should be treated as tracker-conformance behaviour. In particular it always
// answers with the compact peer form, and it never rate-limits or expires
// peers.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9998", "address to listen on")
	interval := flag.Int("interval", 5, "announce interval in seconds")
	flag.Parse()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "devtracker: %v\n", err)
		os.Exit(1)
	}

	t := &tracker{
		interval: time.Duration(*interval) * time.Second,
		swarms:   make(map[string]map[string]peer),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/announce", t.announce)

	// The listening address is printed so a harness that binds port 0 can
	// discover which port the kernel picked.
	fmt.Printf("devtracker: listening on %s\n", ln.Addr())
	if err := http.Serve(ln, mux); err != nil {
		fmt.Fprintf(os.Stderr, "devtracker: %v\n", err)
		os.Exit(1)
	}
}

// peer is one announcing swarm member.
type peer struct {
	ip   net.IP
	port uint16
	left int64
}

// tracker keeps exactly one swarm per info-hash.
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

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	t.mu.Lock()
	swarm := t.swarms[infoHash]
	if swarm == nil {
		swarm = make(map[string]peer)
		t.swarms[infoHash] = swarm
	}
	if q.Get("event") == "stopped" {
		delete(swarm, peerID)
	} else {
		swarm[peerID] = peer{ip: net.ParseIP(host), port: uint16(port), left: left}
	}

	// left=0 marks a seeder, anything else a leecher: that distinction is what
	// complete/incomplete report and what lets a leecher find the seed.
	complete, incomplete := 0, 0
	var compact bytes.Buffer
	for id, p := range swarm {
		if p.left == 0 {
			complete++
		} else {
			incomplete++
		}
		if id == peerID {
			continue // a peer is never handed back to itself
		}
		if v4 := p.ip.To4(); v4 != nil {
			compact.Write(v4)
			compact.Write([]byte{byte(p.port >> 8), byte(p.port)})
		}
	}
	t.mu.Unlock()

	w.Header().Set("Content-Type", "text/plain")
	if _, err := w.Write(response(t.interval, complete, incomplete, compact.Bytes())); err != nil {
		fmt.Fprintf(os.Stderr, "devtracker: write response: %v\n", err)
	}
}

// response renders the bencoded announce reply by hand. Dictionary keys are
// emitted in sorted order so strict bencode decoders accept it.
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
