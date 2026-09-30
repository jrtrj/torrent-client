package tracker

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"torrent-client/internal/bencode"
)

// maxResponseBytes caps a tracker reply, so a hostile or broken endpoint can't
// exhaust memory.
const maxResponseBytes = 4 << 20

// HTTPTracker announces with the HTTP GET form of the protocol.
type HTTPTracker struct {
	URL    string
	Client *http.Client
}

// NewHTTP returns an HTTP tracker client for one announce URL.
func NewHTTP(announceURL string) *HTTPTracker {
	return &HTTPTracker{
		URL:    announceURL,
		Client: &http.Client{Timeout: 30 * time.Second},
	}
}

// Announce performs one announce. Transport failures and 5xx replies come back
// as transient errors, so the retry policy can act on them.
func (t *HTTPTracker) Announce(ctx context.Context, req AnnounceRequest) (AnnounceResponse, error) {
	endpoint, err := t.buildURL(req)
	if err != nil {
		return AnnounceResponse{}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return AnnounceResponse{}, &Error{Transient: false, Err: err}
	}
	httpReq.Header.Set("User-Agent", "torrent-client/0.1")

	resp, err := t.client().Do(httpReq)
	if err != nil {
		return AnnounceResponse{}, &Error{Transient: true, Err: fmt.Errorf("announce %s: %w", t.URL, err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return AnnounceResponse{}, &Error{
			Transient: resp.StatusCode >= 500,
			Err:       fmt.Errorf("announce %s: HTTP %s", t.URL, resp.Status),
		}
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return AnnounceResponse{}, &Error{Transient: true, Err: fmt.Errorf("announce %s: %w", t.URL, err)}
	}
	ar, err := parseResponse(body)
	if err != nil {
		return AnnounceResponse{}, &Error{Transient: false, Err: fmt.Errorf("announce %s: %w", t.URL, err)}
	}
	return ar, nil
}

func (t *HTTPTracker) client() *http.Client {
	if t.Client != nil {
		return t.Client
	}
	return http.DefaultClient
}

// buildURL assembles the announce query. url.Values escapes the way standard
// form decoding expects, which is what trackers do, so a raw binary info-hash or
// peer-id round-trips intact: no bare '+' and no raw spaces on the wire.
func (t *HTTPTracker) buildURL(req AnnounceRequest) (string, error) {
	base, err := url.Parse(t.URL)
	if err != nil {
		return "", &Error{Transient: false, Err: fmt.Errorf("tracker URL %q: %w", t.URL, err)}
	}

	q := base.Query()
	q.Set("info_hash", string(req.InfoHash[:]))
	q.Set("peer_id", string(req.PeerID[:]))
	q.Set("port", strconv.Itoa(int(req.Port)))
	q.Set("uploaded", strconv.FormatInt(req.Uploaded, 10))
	q.Set("downloaded", strconv.FormatInt(req.Downloaded, 10))
	q.Set("left", strconv.FormatInt(req.Left, 10))
	q.Set("compact", "1")
	if req.NumWant > 0 {
		q.Set("numwant", strconv.Itoa(req.NumWant))
	}
	if req.Event != "" {
		q.Set("event", string(req.Event))
	}
	base.RawQuery = q.Encode()
	return base.String(), nil
}

// parseResponse decodes a tracker reply. Peers come as a compact binary blob or a
// list of dictionaries, under a key possibly prefixed with the requesting peer id.
func parseResponse(body []byte) (AnnounceResponse, error) {
	decoded, err := bencode.Decode(bytes.NewReader(body))
	if err != nil {
		return AnnounceResponse{}, fmt.Errorf("tracker response is not bencode: %w", err)
	}
	dict, ok := decoded.(map[string]any)
	if !ok {
		return AnnounceResponse{}, fmt.Errorf("tracker response is %T, want a dictionary", decoded)
	}
	if reason, ok := dict["failure reason"].(string); ok && reason != "" {
		return AnnounceResponse{}, fmt.Errorf("tracker refused the announce: %s", reason)
	}

	var ar AnnounceResponse
	if v, ok := intField(dict, "interval"); ok {
		ar.Interval = time.Duration(v) * time.Second
	}
	if v, ok := intField(dict, "min interval"); ok {
		ar.MinInterval = time.Duration(v) * time.Second
	}
	if v, ok := intField(dict, "complete"); ok {
		ar.Complete = int(v)
	}
	if v, ok := intField(dict, "incomplete"); ok {
		ar.Incomplete = int(v)
	}
	peers, err := parsePeers(dict)
	if err != nil {
		return AnnounceResponse{}, err
	}
	ar.Peers = peers
	return ar, nil
}

// parsePeers walks every key that names a peer list. The `t<id-prefix>peers`
// spelling some trackers use is matched by the suffix rule.
func parsePeers(dict map[string]any) ([]Peer, error) {
	var peers []Peer
	for key, val := range dict {
		switch {
		case key == "peers" || (strings.HasSuffix(key, "peers") && key != "peers6"):
			got, err := decodePeers(val)
			if err != nil {
				return nil, fmt.Errorf("key %q: %w", key, err)
			}
			peers = append(peers, got...)
		case key == "peers6":
			got, err := decodePeers6(val)
			if err != nil {
				return nil, fmt.Errorf("key %q: %w", key, err)
			}
			peers = append(peers, got...)
		}
	}
	return peers, nil
}

// decodePeers handles the compact (6 bytes per peer) and dictionary-list
// spellings of `peers`.
func decodePeers(val any) ([]Peer, error) {
	switch v := val.(type) {
	case string:
		if len(v)%6 != 0 {
			return nil, fmt.Errorf("compact peers is %d bytes, want a multiple of 6", len(v))
		}
		peers := make([]Peer, 0, len(v)/6)
		for i := 0; i+6 <= len(v); i += 6 {
			port := binary.BigEndian.Uint16([]byte(v[i+4 : i+6]))
			if port == 0 {
				continue
			}
			peers = append(peers, Peer{IP: net.IPv4(v[i], v[i+1], v[i+2], v[i+3]), Port: port})
		}
		return peers, nil
	case []any:
		var peers []Peer
		for _, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("peer entry is %T, want a dictionary", item)
			}
			ipStr, _ := m["ip"].(string)
			ip := net.ParseIP(ipStr)
			port, ok := intField(m, "port")
			if ip == nil || !ok || port < 1 || port > 65535 {
				continue
			}
			peers = append(peers, Peer{IP: ip, Port: uint16(port)})
		}
		return peers, nil
	default:
		return nil, fmt.Errorf("peers is %T, want a binary blob or a list", val)
	}
}

// decodePeers6 handles the IPv6 compact form: 18 bytes per peer (16-byte address
// plus 2-byte port).
func decodePeers6(val any) ([]Peer, error) {
	s, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("peers6 is %T, want a binary blob", val)
	}
	if len(s)%18 != 0 {
		return nil, fmt.Errorf("compact peers6 is %d bytes, want a multiple of 18", len(s))
	}
	var peers []Peer
	for i := 0; i+18 <= len(s); i += 18 {
		port := binary.BigEndian.Uint16([]byte(s[i+16 : i+18]))
		if port == 0 {
			continue
		}
		ip := make(net.IP, net.IPv6len)
		copy(ip, s[i:i+16])
		peers = append(peers, Peer{IP: ip, Port: port})
	}
	return peers, nil
}

// intField reads a bencode integer as int64. The generic decoder yields int64
// or uint64 depending on the value, so both are accepted.
func intField(m map[string]any, key string) (int64, bool) {
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int64:
		return n, true
	case uint64:
		return int64(n), true
	}
	return 0, false
}
