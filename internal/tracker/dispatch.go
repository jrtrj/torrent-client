package tracker

import (
	"fmt"
	"net/url"
	"strings"
)

// New returns the transport for one announce URL, picked by scheme: http and
// https speak the HTTP protocol, udp speaks BEP 15. This is the only place
// they're told apart, so the engine just keeps taking the Tracker interface.
func New(announceURL string) (Tracker, error) {
	u, err := url.Parse(strings.TrimSpace(announceURL))
	if err != nil {
		return nil, fmt.Errorf("tracker URL %q: %w", announceURL, err)
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		return NewHTTP(announceURL), nil
	case "udp":
		return NewUDP(announceURL)
	default:
		return nil, fmt.Errorf("tracker URL %q: unsupported scheme %q", announceURL, u.Scheme)
	}
}
