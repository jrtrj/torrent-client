package tracker

import (
	"fmt"
	"net/url"
	"strings"
)

// New returns the transport for one announce URL, chosen by its scheme:
// http and https use the HTTP protocol, udp uses BEP 15. This is the single
// place the two transports are told apart, so the engine keeps taking the
// Tracker interface and never learns which wire protocol a swarm speaks.
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
