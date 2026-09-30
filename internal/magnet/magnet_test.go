package magnet

import (
	"encoding/base32"
	"encoding/hex"
	"strings"
	"testing"
)

// hash is a fixed 20-byte info-hash used across the cases.
var hash = [20]byte{
	0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc,
	0xba, 0x98, 0x76, 0x54, 0x32, 0x10, 0x0f, 0x1e, 0x2d, 0x3c,
}

func hexHash() string { return hex.EncodeToString(hash[:]) }
func base32Hash() string {
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(hash[:])
}

func TestParseReadsAHexInfoHash(t *testing.T) {
	m, err := Parse("magnet:?xt=urn:btih:" + hexHash() + "&tr=http://t/announce")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if m.InfoHash != hash {
		t.Fatalf("info-hash = %x, want %x", m.InfoHash, hash)
	}
}

// The base32 form appears in older links and must decode to the same 20 bytes.
func TestParseReadsABase32InfoHash(t *testing.T) {
	m, err := Parse("magnet:?xt=urn:btih:" + base32Hash() + "&tr=http://t/announce")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if m.InfoHash != hash {
		t.Fatalf("info-hash = %x, want %x", m.InfoHash, hash)
	}
}

func TestParseReadsBothEncodingsToTheSameHash(t *testing.T) {
	a, err := Parse("magnet:?xt=urn:btih:" + hexHash() + "&tr=http://t/announce")
	if err != nil {
		t.Fatal(err)
	}
	b, err := Parse("magnet:?xt=urn:btih:" + base32Hash() + "&tr=http://t/announce")
	if err != nil {
		t.Fatal(err)
	}
	if a.InfoHash != b.InfoHash {
		t.Fatalf("hex gave %x but base32 gave %x", a.InfoHash, b.InfoHash)
	}
}

func TestParseCollectsTrackersAndDisplayName(t *testing.T) {
	uri := "magnet:?xt=urn:btih:" + hexHash() +
		"&dn=My%20File.iso" +
		"&tr=" + "udp%3A%2F%2Ft1%3A80%2Fannounce" +
		"&tr=" + "http%3A%2F%2Ft2%3A6969%2Fannounce" +
		"&xl=12345"

	m, err := Parse(uri)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if m.DisplayName != "My File.iso" {
		t.Errorf("display name = %q, want %q", m.DisplayName, "My File.iso")
	}
	want := []string{"udp://t1:80/announce", "http://t2:6969/announce"}
	if len(m.Trackers) != len(want) {
		t.Fatalf("trackers = %v, want %v", m.Trackers, want)
	}
	for i := range want {
		if m.Trackers[i] != want[i] {
			t.Errorf("tracker %d = %q, want %q", i, m.Trackers[i], want[i])
		}
	}
}

// The tracker list may arrive as one comma- or repeat-separated value; the
// repeat form above is the common one, and blank entries are dropped.
func TestParseDropsBlankTrackers(t *testing.T) {
	m, err := Parse("magnet:?xt=urn:btih:" + hexHash() + "&tr=&tr=http://t/announce&tr=")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(m.Trackers) != 1 || m.Trackers[0] != "http://t/announce" {
		t.Fatalf("trackers = %v, want just the one non-blank entry", m.Trackers)
	}
}

func TestParseRefuses(t *testing.T) {
	cases := []struct {
		name string
		uri  string
	}{
		{"not a magnet URI", "http://example.com/x.torrent"},
		{"no xt at all", "magnet:?dn=thing&tr=http://t/announce"},
		{"wrong urn namespace", "magnet:?xt=urn:sha1:" + hexHash() + "&tr=http://t/announce"},
		{"hex hash too short", "magnet:?xt=urn:btih:0123&tr=http://t/announce"},
		{"hex hash not hex", "magnet:?xt=urn:btih:" + strings.Repeat("z", 40) + "&tr=http://t/announce"},
		{"base32 wrong length", "magnet:?xt=urn:btih:" + base32Hash()[:20] + "&tr=http://t/announce"},
		// DHT is out of scope, so a trackerless magnet is refused up front
		// rather than accepted and found unresolvable.
		{"no tracker", "magnet:?xt=urn:btih:" + hexHash()},
		{"only blank trackers", "magnet:?xt=urn:btih:" + hexHash() + "&tr="},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if m, err := Parse(tc.uri); err == nil {
				t.Fatalf("Parse accepted %q and returned %+v", tc.uri, m)
			}
		})
	}
}

// A magnet may carry several xt entries; the first usable one wins.
func TestParseUsesTheFirstUsableXT(t *testing.T) {
	m, err := Parse("magnet:?xt=urn:sha1:deadbeef&xt=urn:btih:" + hexHash() + "&tr=http://t/announce")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if m.InfoHash != hash {
		t.Fatalf("info-hash = %x, want %x", m.InfoHash, hash)
	}
}
