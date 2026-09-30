package ui

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"torrent-client/internal/engine"
)

const (
	barCapLeft  = "\u2595" // RIGHT ONE EIGHTH BLOCK: left cap
	barCapRight = "\u258f" // LEFT ONE EIGHTH BLOCK: right cap
	barFull     = "\u2588"
	barEmpty    = "\u2591"

	// barCells is the interior bar width on a normal terminal; Render shrinks
	// it when the line has to fit somewhere narrower.
	barCells = 16
	// minBarCells: below this the bar says less than the percentage printed
	// next to it anyway, so Render won't shrink it further.
	minBarCells = 1
)

// fractionOf is the completed share of the torrent, clamped to 0..1 so a
// snapshot that overshoots — bytes counted for pieces not yet verified — can't
// render a bar longer than its track.
func fractionOf(st engine.Stats) float64 {
	if st.BytesTotal <= 0 {
		return 0
	}
	f := float64(st.BytesDone) / float64(st.BytesTotal)
	switch {
	case f < 0:
		return 0
	case f > 1:
		return 1
	}
	return f
}

// Bar renders the progress track for fraction (clamped to 0..1) with cells
// interior cells. The caps are always there and count towards the length, so
// the result is cells+2 glyphs wide.
func Bar(fraction float64, cells int) string {
	if cells < minBarCells {
		cells = minBarCells
	}
	switch {
	case fraction < 0:
		fraction = 0
	case fraction > 1:
		fraction = 1
	}
	filled := int(fraction*float64(cells) + 0.5)
	if filled > cells {
		filled = cells
	}
	var b strings.Builder
	b.WriteString(barCapLeft)
	b.WriteString(strings.Repeat(barFull, filled))
	b.WriteString(strings.Repeat(barEmpty, cells-filled))
	b.WriteString(barCapRight)
	return b.String()
}

// Percent is completed over total as a whole number. The engine counts only
// verified bytes, so this is progress, never throughput.
func Percent(done, total int64) string {
	if total <= 0 {
		return "0%"
	}
	pct := done * 100 / total
	if pct > 100 {
		pct = 100
	}
	if pct < 0 {
		pct = 0
	}
	return fmt.Sprintf("%d%%", pct)
}

// HumanRate formats a bytes-per-second rate as the design's "1.8 MB/s": binary
// units, one decimal place, so the field holds a steady width from frame to
// frame instead of jittering.
func HumanRate(bytesPerSec float64) string {
	if bytesPerSec < 0 {
		bytesPerSec = 0
	}
	switch {
	case bytesPerSec < 1024:
		return fmt.Sprintf("%.0f B/s", bytesPerSec)
	case bytesPerSec < 1024*1024:
		return fmt.Sprintf("%.1f kB/s", bytesPerSec/1024)
	case bytesPerSec < 1024*1024*1024:
		return fmt.Sprintf("%.1f MB/s", bytesPerSec/(1024*1024))
	default:
		return fmt.Sprintf("%.2f GB/s", bytesPerSec/(1024*1024*1024))
	}
}

// ETA is the time left for remaining bytes at rate. A rate of zero means "not
// known yet" — inventing a countdown from no throughput would be a lie — and
// zero or negative remaining means we're done.
func ETA(remaining int64, rate float64) string {
	if remaining <= 0 {
		return "00:00:00"
	}
	if rate <= 0 {
		return "--:--:--"
	}
	secs := int64(float64(remaining)/rate + 0.5)
	if secs < 0 {
		secs = 0
	}
	// Past 99 hours the figure stops meaning anything, and letting it grow
	// would widen the field, so it's capped.
	if secs > 99*3600+59*60+59 {
		return "99:59:59"
	}
	return fmt.Sprintf("%02d:%02d:%02d", secs/3600, (secs%3600)/60, secs%60)
}

// Truncate cuts s to at most width display columns without splitting a rune.
// The bar is block glyphs, several bytes each, so a byte-wise cut would put
// half a character on screen.
func Truncate(s string, width int) string {
	if width <= 0 {
		return ""
	}
	if utf8.RuneCountInString(s) <= width {
		return s
	}
	var b strings.Builder
	n := 0
	for _, r := range s {
		if n == width {
			break
		}
		b.WriteRune(r)
		n++
	}
	return b.String()
}

// LimitBadge names the active bandwidth caps, or "" when neither direction is
// capped. Naming only the capped directions keeps the field short enough to
// survive a narrow terminal; an unlimited run shows nothing at all.
func LimitBadge(st engine.Stats) string {
	var b strings.Builder
	b.WriteString("cap")
	if st.MaxDownRate > 0 {
		b.WriteString(" \u2193" + HumanRate(float64(st.MaxDownRate)))
	}
	if st.MaxUpRate > 0 {
		b.WriteString(" \u2191" + HumanRate(float64(st.MaxUpRate)))
	}
	if b.Len() == len("cap") {
		return ""
	}
	return b.String()
}

// Render builds one sticky-line frame from an engine snapshot and a
// caller-smoothed bytes/second rate, limited to width columns. It's pure — no
// terminal, no clock, no state — which is why the rendering tests drive it
// directly.
//
// Fields go in priority order: rate, ETA, workers, pieces, caps. Narrowing
// drops the least useful first, then shrinks the bar, then cuts rune-safely.
func Render(st engine.Stats, rate float64, width int) string {
	// One column stays free: writing into the final column puts many
	// terminals into "pending wrap", and the next byte moves to the next
	// row — the exact smear this display exists to avoid.
	budget := width - 1
	if budget < 1 {
		budget = 1
	}

	percent := Percent(st.BytesDone, st.BytesTotal)
	head := Bar(fractionOf(st), barCells) + " " + percent
	line := head

	fields := []string{
		HumanRate(rate),
		"ETA " + ETA(st.BytesTotal-st.BytesDone, rate),
		fmt.Sprintf("%d/%d workers", st.PeersActive, len(st.Peers)),
		fmt.Sprintf("%d pieces", st.PiecesDone),
		LimitBadge(st),
	}
	for _, f := range fields {
		if f == "" {
			break
		}
		candidate := line + " | " + f
		if utf8.RuneCountInString(candidate) > budget {
			break
		}
		line = candidate
	}
	if utf8.RuneCountInString(line) <= budget {
		return line
	}

	// Even the bar and percentage overflow: shrink the bar to whatever room
	// is left, then cut as a last resort. minBarCells keeps a cell or two of
	// bar even when that means cutting into the percentage.
	suffix := " " + percent
	room := budget - utf8.RuneCountInString(suffix) - 2 // the two caps
	if room < minBarCells {
		room = minBarCells
	}
	return Truncate(Bar(fractionOf(st), room)+suffix, budget)
}
