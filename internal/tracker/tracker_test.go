package tracker

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"torrent-client/internal/bencode"
)

func reply(t *testing.T, fields map[string]any) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := bencode.Marshal(&buf, fields); err != nil {
		t.Fatalf("marshal tracker reply: %v", err)
	}
	return buf.Bytes()
}

func serveTracker(t *testing.T, handler http.HandlerFunc) *HTTPTracker {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return NewHTTP(srv.URL)
}

func testRequest() AnnounceRequest {
	var infoHash, peerID [20]byte
	for i := range infoHash {
		infoHash[i] = byte(i)
	}
	copy(peerID[:], "-TC0001-abcdefghijkl")
	return AnnounceRequest{InfoHash: infoHash, PeerID: peerID, Port: 6881, Downloaded: 10, Left: 12345, NumWant: 50}
}

func TestAnnounceParsesCompactPeers(t *testing.T) {
	// 127.0.0.1:8080 followed by 10.0.0.7:0, which must be dropped.
	compact := string([]byte{127, 0, 0, 1, 0x1f, 0x90, 10, 0, 0, 7, 0, 0})
	tr := serveTracker(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(reply(t, map[string]any{"interval": 1800, "complete": 1, "incomplete": 2, "peers": compact}))
	})

	resp, err := tr.Announce(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("Announce: %v", err)
	}
	if len(resp.Peers) != 1 {
		t.Fatalf("peers = %v, want exactly one", resp.Peers)
	}
	if got := resp.Peers[0].Addr(); got != "127.0.0.1:8080" {
		t.Fatalf("peer = %s, want 127.0.0.1:8080", got)
	}
	if resp.Interval != 1800*time.Second {
		t.Fatalf("interval = %s, want 30m", resp.Interval)
	}
	if resp.Complete != 1 || resp.Incomplete != 2 {
		t.Fatalf("complete/incomplete = %d/%d, want 1/2", resp.Complete, resp.Incomplete)
	}
}

func TestAnnounceParsesDictionaryPeers(t *testing.T) {
	peers := []any{
		map[string]any{"ip": "10.0.0.5", "port": 51413},
		map[string]any{"ip": "not-an-ip", "port": 1},
	}
	tr := serveTracker(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(reply(t, map[string]any{"interval": 60, "peers": peers}))
	})

	resp, err := tr.Announce(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("Announce: %v", err)
	}
	if len(resp.Peers) != 1 || resp.Peers[0].Addr() != "10.0.0.5:51413" {
		t.Fatalf("peers = %v, want 10.0.0.5:51413", resp.Peers)
	}
}

// Some trackers key the peer list by a prefix of the requesting peer id.
func TestAnnounceParsesIDPrefixedPeersKey(t *testing.T) {
	compact := string([]byte{192, 168, 1, 9, 0x1a, 0xe1})
	tr := serveTracker(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(reply(t, map[string]any{"interval": 60, "-TC000peers": compact}))
	})

	resp, err := tr.Announce(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("Announce: %v", err)
	}
	if len(resp.Peers) != 1 || resp.Peers[0].Addr() != "192.168.1.9:6881" {
		t.Fatalf("peers = %v, want 192.168.1.9:6881", resp.Peers)
	}
}

func TestAnnounceParsesPeers6(t *testing.T) {
	blob := string(append(net.ParseIP("::1").To16(), 0x1f, 0x90))
	tr := serveTracker(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(reply(t, map[string]any{"interval": 60, "peers": "", "peers6": blob}))
	})

	resp, err := tr.Announce(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("Announce: %v", err)
	}
	if len(resp.Peers) != 1 || resp.Peers[0].Addr() != "[::1]:8080" {
		t.Fatalf("peers = %v, want [::1]:8080", resp.Peers)
	}
}

// The info-hash and peer id are raw binary. A byte like '+' or ' ' must not be
// mangled into a space or a bare plus on the way to the tracker.
func TestAnnounceEncodesBinaryHashAndPeerIDIntact(t *testing.T) {
	var infoHash, peerID [20]byte
	for i := range infoHash {
		infoHash[i] = byte(i * 13)
	}
	copy(peerID[:], "+++   \x00\xff-abcdefghij")
	peerID[0], peerID[1], peerID[2] = '+', '+', ' '

	var got url.Values
	tr := serveTracker(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Query()
		w.Write(reply(t, map[string]any{"interval": 60, "peers": ""}))
	})

	if _, err := tr.Announce(context.Background(), AnnounceRequest{InfoHash: infoHash, PeerID: peerID, Port: 6881}); err != nil {
		t.Fatalf("Announce: %v", err)
	}
	if got.Get("info_hash") != string(infoHash[:]) {
		t.Fatalf("info_hash round trip = %q, want %q", got.Get("info_hash"), string(infoHash[:]))
	}
	if got.Get("peer_id") != string(peerID[:]) {
		t.Fatalf("peer_id round trip = %q, want %q", got.Get("peer_id"), string(peerID[:]))
	}
	if got.Get("compact") != "1" || got.Get("port") != "6881" {
		t.Fatalf("query = %v", got)
	}
}

func TestAnnounceSendsEventAndSkipsEmptyOnes(t *testing.T) {
	var got url.Values
	tr := serveTracker(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Query()
		w.Write(reply(t, map[string]any{"interval": 60, "peers": ""}))
	})

	req := testRequest()
	req.Event = EventStarted
	if _, err := tr.Announce(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got.Get("event") != "started" {
		t.Fatalf("event = %q, want started", got.Get("event"))
	}

	req.Event = ""
	if _, err := tr.Announce(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, present := got["event"]; present {
		t.Fatalf("a periodic announce sent event=%q", got.Get("event"))
	}
}

func TestAnnounceFailureReasonIsFatal(t *testing.T) {
	tr := serveTracker(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(reply(t, map[string]any{"failure reason": "unregistered torrent"}))
	})

	_, err := tr.Announce(context.Background(), testRequest())
	if err == nil {
		t.Fatal("a failure reason was not reported as an error")
	}
	if !IsFatal(err) {
		t.Fatalf("err = %v, want a fatal tracker error", err)
	}
}

func TestAnnounceStatusCodesClassifyFailures(t *testing.T) {
	tests := []struct {
		status int
		fatal  bool
	}{
		{http.StatusServiceUnavailable, false},
		{http.StatusNotFound, true},
	}
	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			tr := serveTracker(t, func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "nope", tt.status)
			})
			_, err := tr.Announce(context.Background(), testRequest())
			if err == nil {
				t.Fatal("the status was not reported as an error")
			}
			if IsFatal(err) != tt.fatal {
				t.Fatalf("IsFatal(%v) = %v, want %v", err, IsFatal(err), tt.fatal)
			}
		})
	}
}

func TestAnnounceRejectsNonBencodeBody(t *testing.T) {
	tr := serveTracker(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("this is not bencode"))
	})
	if _, err := tr.Announce(context.Background(), testRequest()); err == nil {
		t.Fatal("a non-bencode body was accepted")
	}
}

func TestAnnounceWithRetryBacksOffThenSucceeds(t *testing.T) {
	calls := 0
	tr := serveTracker(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			http.Error(w, "try later", http.StatusServiceUnavailable)
			return
		}
		w.Write(reply(t, map[string]any{"interval": 60, "peers": ""}))
	})

	resp, err := AnnounceWithRetry(context.Background(), tr, testRequest(),
		RetryPolicy{Attempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("AnnounceWithRetry: %v", err)
	}
	if calls != 2 {
		t.Fatalf("attempts = %d, want 2", calls)
	}
	if resp.Interval != time.Minute {
		t.Fatalf("interval = %s, want 1m", resp.Interval)
	}
}

func TestAnnounceWithRetryGivesUpOnTransientFailures(t *testing.T) {
	calls := 0
	tr := serveTracker(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, "try later", http.StatusServiceUnavailable)
	})

	if _, err := AnnounceWithRetry(context.Background(), tr, testRequest(),
		RetryPolicy{Attempts: 2, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}); err == nil {
		t.Fatal("AnnounceWithRetry succeeded against a permanently failing tracker")
	}
	if calls != 2 {
		t.Fatalf("attempts = %d, want 2", calls)
	}
}

func TestAnnounceWithRetryDoesNotRetryFatalErrors(t *testing.T) {
	calls := 0
	tr := serveTracker(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Write(reply(t, map[string]any{"failure reason": "bad info hash"}))
	})

	if _, err := AnnounceWithRetry(context.Background(), tr, testRequest(),
		RetryPolicy{Attempts: 5, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}); err == nil {
		t.Fatal("a fatal tracker error was swallowed")
	}
	if calls != 1 {
		t.Fatalf("attempts = %d, want exactly 1", calls)
	}
}

func TestAnnounceWithRetryHonoursContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tr := serveTracker(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	})

	_, err := AnnounceWithRetry(ctx, tr, testRequest(),
		RetryPolicy{Attempts: 5, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})
	if err == nil {
		t.Fatal("a cancelled context was ignored")
	}
}
