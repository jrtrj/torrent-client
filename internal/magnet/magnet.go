// Package magnet parses magnet URIs (BEP 9's xt=urn:btih form).
//
// A magnet link carries no metadata: just the info-hash and where to look. The
// metadata itself is fetched from peers over ut_metadata, which lives in
// internal/metadata. DHT is out of scope for this client, so a magnet that
// names no tracker is refused rather than accepted and later found
// unresolvable.
package magnet

import (
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
)

// InfoHashSize is the length of a BitTorrent info-hash.
const InfoHashSize = 20

// Magnet is a parsed magnet URI.
type Magnet struct {
	InfoHash    [InfoHashSize]byte
	DisplayName string
	Trackers    []string
}

// Parse decodes a magnet URI.
func Parse(uri string) (*Magnet, error) {
	const prefix = "magnet:?"
	if !strings.HasPrefix(uri, prefix) {
		return nil, fmt.Errorf("magnet: %q is not a magnet URI", uri)
	}
	values, err := url.ParseQuery(uri[len(prefix):])
	if err != nil {
		return nil, fmt.Errorf("magnet: malformed query: %w", err)
	}

	var m Magnet
	for _, xt := range values["xt"] {
		hash, err := parseXT(xt)
		if err != nil {
			continue
		}
		m.InfoHash = hash
		break
	}
	if m.InfoHash == ([InfoHashSize]byte{}) {
		return nil, fmt.Errorf("magnet: no usable xt=urn:btih info-hash")
	}

	// dn is a display name only; xl and other keys are accepted and ignored.
	m.DisplayName = values.Get("dn")
	for _, tr := range values["tr"] {
		if tr = strings.TrimSpace(tr); tr != "" {
			m.Trackers = append(m.Trackers, tr)
		}
	}
	if len(m.Trackers) == 0 {
		return nil, fmt.Errorf("magnet: no tracker, and DHT is out of scope for this client")
	}
	return &m, nil
}

// parseXT accepts both encodings seen in the wild: the 40-character hex form
// and the 32-character base32 form, normalising either to 20 raw bytes.
func parseXT(xt string) ([InfoHashSize]byte, error) {
	var out [InfoHashSize]byte
	const prefix = "urn:btih:"
	if !strings.HasPrefix(xt, prefix) {
		return out, fmt.Errorf("unsupported xt %q", xt)
	}
	h := strings.TrimSpace(xt[len(prefix):])

	switch len(h) {
	case 2 * InfoHashSize:
		b, err := hex.DecodeString(h)
		if err != nil {
			return out, fmt.Errorf("xt hex: %w", err)
		}
		copy(out[:], b)
		return out, nil
	case 32:
		b, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(h))
		if err != nil {
			return out, fmt.Errorf("xt base32: %w", err)
		}
		if len(b) != InfoHashSize {
			return out, fmt.Errorf("xt base32 decoded to %d bytes, want %d", len(b), InfoHashSize)
		}
		copy(out[:], b)
		return out, nil
	default:
		return out, fmt.Errorf("xt hash is %d characters, want 40 hex or 32 base32", len(h))
	}
}
