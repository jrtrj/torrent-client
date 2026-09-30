package ui

import (
	"strings"
	"testing"
	"unicode/utf8"

	"torrent-client/internal/engine"
)

func TestBarRendersBothCapsAndTheRequestedCells(t *testing.T) {
	tests := []struct {
		name     string
		fraction float64
		cells    int
		full     int
	}{
		{"empty", 0, 16, 0},
		{"half", 0.5, 16, 8},
		{"full", 1, 16, 16},
		{"negative clamps to empty", -3, 8, 0},
		{"above one clamps to full", 7, 8, 8},
		{"single cell", 1, 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Bar(tt.fraction, tt.cells)
			if n := utf8.RuneCountInString(got); n != tt.cells+2 {
				t.Fatalf("Bar(%v, %d) = %q: %d runes, want %d (cells plus two caps)",
					tt.fraction, tt.cells, got, n, tt.cells+2)
			}
			if !strings.HasPrefix(got, barCapLeft) || !strings.HasSuffix(got, barCapRight) {
				t.Fatalf("Bar(%v, %d) = %q, want both caps", tt.fraction, tt.cells, got)
			}
			if n := strings.Count(got, barFull); n != tt.full {
				t.Fatalf("Bar(%v, %d) = %q has %d filled cells, want %d",
					tt.fraction, tt.cells, got, n, tt.full)
			}
		})
	}
}

// A zero or negative cell count would otherwise render a bar with no track at
// all, which reads as "no progress" next to a percentage saying otherwise.
func TestBarKeepsAMinimumTrack(t *testing.T) {
	for _, cells := range []int{-1, 0} {
		if got := Bar(0.5, cells); utf8.RuneCountInString(got) != minBarCells+2 {
			t.Fatalf("Bar(0.5, %d) = %q, want %d interior cells",
				cells, got, minBarCells)
		}
	}
}

func TestPercent(t *testing.T) {
	tests := []struct {
		done, total int64
		want        string
	}{
		{0, 100, "0%"},
		{50, 100, "50%"},
		{100, 100, "100%"},
		{150, 100, "100%"}, // bytes can overshoot the total; the figure must not
		{1, 3, "33%"},
		{5, 0, "0%"}, // unknown total
		{-5, 100, "0%"},
	}
	for _, tt := range tests {
		if got := Percent(tt.done, tt.total); got != tt.want {
			t.Errorf("Percent(%d, %d) = %q, want %q", tt.done, tt.total, got, tt.want)
		}
	}
}

func TestHumanRate(t *testing.T) {
	tests := []struct {
		in   float64
		want string
	}{
		{0, "0 B/s"},
		{512, "512 B/s"},
		{1023, "1023 B/s"},
		{1024, "1.0 kB/s"},
		{1536, "1.5 kB/s"},
		{1048576, "1.0 MB/s"},
		{1887436.8, "1.8 MB/s"},
		{1073741824, "1.00 GB/s"},
		{-10, "0 B/s"}, // a negative rate is nonsense; do not print it
	}
	for _, tt := range tests {
		if got := HumanRate(tt.in); got != tt.want {
			t.Errorf("HumanRate(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestETA(t *testing.T) {
	tests := []struct {
		name      string
		remaining int64
		rate      float64
		want      string
	}{
		{"finished", 0, 1000, "00:00:00"},
		{"negative remaining", -1, 1000, "00:00:00"},
		{"no rate yet is a placeholder, not a guess", 1000, 0, "--:--:--"},
		{"negative rate", 1000, -5, "--:--:--"},
		{"one second", 1000, 1000, "00:00:01"},
		{"three minutes", 192 * 1024, 1024, "00:03:12"},
		{"absurd duration is capped", 1 << 40, 1, "99:59:59"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ETA(tt.remaining, tt.rate); got != tt.want {
				t.Fatalf("ETA(%d, %v) = %q, want %q", tt.remaining, tt.rate, got, tt.want)
			}
		})
	}
}

// The bar is multi-byte glyphs, so a byte-wise cut would put half a character on
// screen. Every prefix has to stay valid UTF-8.
func TestTruncateNeverSplitsARune(t *testing.T) {
	line := barCapLeft + strings.Repeat(barFull, 3) + barCapRight

	if got := Truncate(line, 3); got != barCapLeft+strings.Repeat(barFull, 2) {
		t.Fatalf("Truncate(line, 3) = %q, want the first three glyphs", got)
	}
	if got := Truncate(line, 0); got != "" {
		t.Fatalf("Truncate(line, 0) = %q, want empty", got)
	}
	if got := Truncate(line, -1); got != "" {
		t.Fatalf("Truncate(line, -1) = %q, want empty", got)
	}
	if got := Truncate(line, 99); got != line {
		t.Fatalf("Truncate(line, 99) = %q, want the line unchanged", got)
	}
	for w := 0; w <= 6; w++ {
		if got := Truncate(line, w); !utf8.ValidString(got) {
			t.Fatalf("Truncate(line, %d) = %q, which is not valid UTF-8", w, got)
		}
	}
}

func renderStats() engine.Stats {
	return engine.Stats{
		Pieces:      30,
		PiecesDone:  12,
		BytesTotal:  30 << 20,
		BytesDone:   12 << 20,
		BytesIn:     12 << 20,
		PeersActive: 4,
		Peers:       make([]engine.PeerStat, 5),
	}
}

// The single hardest requirement: whatever the width, the frame has to fit and
// carry no escape sequences. Writing into the final column is what puts
// terminals into pending-wrap, so the frame stays inside the budget.
func TestRenderFitsEveryWidthWithoutEscapes(t *testing.T) {
	st := renderStats()
	for width := 1; width <= 200; width++ {
		out := Render(st, 1.8*1024*1024, width)
		budget := width - 1
		if budget < 1 {
			budget = 1
		}
		if n := utf8.RuneCountInString(out); n > budget {
			t.Fatalf("width %d: frame is %d runes, want <= %d: %q", width, n, budget, out)
		}
		if strings.ContainsRune(out, 0x1b) {
			t.Fatalf("width %d: frame carries an escape sequence: %q", width, out)
		}
		if !utf8.ValidString(out) {
			t.Fatalf("width %d: frame is not valid UTF-8: %q", width, out)
		}
	}
}

// Narrowing the terminal has to drop the least useful fields first and never
// lose the bar and percentage entirely.
func TestRenderDropsFieldsAsTheTerminalNarrows(t *testing.T) {
	st := renderStats()

	wide := Render(st, 1.8*1024*1024, 120)
	for _, want := range []string{"MB/s", "ETA ", "workers", "pieces", "%", barCapLeft} {
		if !strings.Contains(wide, want) {
			t.Fatalf("wide frame %q is missing %q", wide, want)
		}
	}

	narrow := Render(st, 1.8*1024*1024, 40)
	if n := utf8.RuneCountInString(narrow); n > 39 {
		t.Fatalf("40-column frame is %d runes, want <= 39: %q", n, narrow)
	}
	for _, want := range []string{"%", barCapLeft} {
		if !strings.Contains(narrow, want) {
			t.Fatalf("40-column frame %q dropped the essential %q", narrow, want)
		}
	}

	tiny := Render(st, 1.8*1024*1024, 8)
	if got := utf8.RuneCountInString(tiny); got > 7 {
		t.Fatalf("8-column frame is %d runes, want <= 7: %q", got, tiny)
	}
}

func TestRenderClampsOvershootToAFullBar(t *testing.T) {
	st := engine.Stats{Pieces: 1, PiecesDone: 1, BytesTotal: 100, BytesDone: 250}
	out := Render(st, 0, 80)
	if !strings.Contains(out, "100%") {
		t.Fatalf("overshoot rendered %q, want 100%%", out)
	}
	if n := strings.Count(out, barFull); n != barCells {
		t.Fatalf("overshoot bar has %d filled cells, want %d", n, barCells)
	}
}

func TestRenderWithNoTotalDoesNotDivideByZero(t *testing.T) {
	out := Render(engine.Stats{}, 0, 80)
	if !strings.Contains(out, "0%") {
		t.Fatalf("empty stats rendered %q, want 0%%", out)
	}
}

func TestLimitBadgeNamesOnlyTheCappedDirections(t *testing.T) {
	tests := []struct {
		name string
		st   engine.Stats
		want string
	}{
		{"unlimited", engine.Stats{}, ""},
		{"down only", engine.Stats{MaxDownRate: 512 * 1024}, "cap \u2193512.0 kB/s"},
		{"up only", engine.Stats{MaxUpRate: 2 * 1024 * 1024}, "cap \u21912.0 MB/s"},
		{"both", engine.Stats{MaxDownRate: 512 * 1024, MaxUpRate: 2 * 1024 * 1024}, "cap \u2193512.0 kB/s \u21912.0 MB/s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LimitBadge(tt.st); got != tt.want {
				t.Fatalf("LimitBadge() = %q, want %q", got, tt.want)
			}
		})
	}
}

// The display has to say limiting is on, and a capped frame still has to obey
// the width budget at every width.
func TestRenderShowsActiveCaps(t *testing.T) {
	st := renderStats()
	st.MaxDownRate = 512 * 1024
	st.MaxUpRate = 2 * 1024 * 1024

	wide := Render(st, 0, 120)
	if !strings.Contains(wide, "cap") || !strings.Contains(wide, "512.0 kB/s") || !strings.Contains(wide, "2.0 MB/s") {
		t.Fatalf("capped frame %q does not name the caps", wide)
	}
	// The limits are the last field, so they drop first on a narrow terminal,
	// but an unlimited frame never shows them at all.
	if got := Render(renderStats(), 0, 120); strings.Contains(got, "cap") {
		t.Fatalf("unlimited frame %q claims a cap", got)
	}

	for width := 1; width <= 200; width++ {
		out := Render(st, 1.8*1024*1024, width)
		budget := width - 1
		if budget < 1 {
			budget = 1
		}
		if n := utf8.RuneCountInString(out); n > budget {
			t.Fatalf("width %d: capped frame is %d runes, want <= %d: %q", width, n, budget, out)
		}
		if !utf8.ValidString(out) {
			t.Fatalf("width %d: capped frame is not valid UTF-8: %q", width, out)
		}
	}
}
