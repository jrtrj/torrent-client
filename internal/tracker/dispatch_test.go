package tracker

import "testing"

func TestNewRoutesByScheme(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{"udp", "udp://tracker.example.org:6969/announce", "udp"},
		{"udp without path", "udp://tracker.example.org:6969", "udp"},
		{"http", "http://tracker.example.org:6969/announce", "http"},
		{"https", "https://tracker.example.org/announce", "http"},
		{"http uppercase", "HTTP://tracker.example.org/announce", "http"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.url)
			if err != nil {
				t.Fatalf("New(%q): %v", tt.url, err)
			}
			switch tt.want {
			case "udp":
				if _, ok := got.(*UDPTracker); !ok {
					t.Fatalf("New(%q) = %T, want *UDPTracker", tt.url, got)
				}
			case "http":
				if _, ok := got.(*HTTPTracker); !ok {
					t.Fatalf("New(%q) = %T, want *HTTPTracker", tt.url, got)
				}
			}
		})
	}
}

func TestNewRejectsUnsupportedSchemes(t *testing.T) {
	for _, url := range []string{
		"ftp://tracker.example.org/announce",
		"wss://tracker.example.org/announce",
		"tracker.example.org/announce",
		"",
	} {
		if _, err := New(url); err == nil {
			t.Fatalf("New(%q) accepted an unsupported scheme", url)
		}
	}
}

func TestNewRejectsUDPWithoutPort(t *testing.T) {
	if _, err := New("udp://tracker.example.org/announce"); err == nil {
		t.Fatal("a udp tracker URL without a port was accepted")
	}
}

// Both transports must satisfy the interface the engine consumes.
func TestTransportsSatisfyTracker(t *testing.T) {
	var _ Tracker = (*HTTPTracker)(nil)
	var _ Tracker = (*UDPTracker)(nil)
}
