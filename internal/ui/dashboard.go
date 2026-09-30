package ui

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"torrent-client/internal/engine"
)

const (
	// redrawInterval is the ~4 Hz cadence the design pinned: fast enough to
	// look live, and with the skip-if-unchanged check below, a still display
	// costs nothing after the first frame.
	redrawInterval = 250 * time.Millisecond
	// defaultWidth is the fallback when neither the ioctl nor COLUMNS answers.
	defaultWidth = 80
	// rateAlpha weights each new instantaneous sample. At 4 Hz this lags the
	// true rate by about a second — enough to stop the number flickering while
	// still tracking a real change in throughput.
	rateAlpha = 0.35
)

// eraseLine returns the cursor to column 0 and blanks the row.
//
// It's erase-line rather than save/restore-cursor: the sticky line is the
// bottom row, so an event printed there can scroll the screen, and a DECSC
// position would then point at unrelated text. Erase-line survives scrolling
// because it's rebuilt from wherever the cursor is on every write.
const eraseLine = "\r\x1b[K"

// Dashboard is the live terminal display: one sticky progress line redrawn in
// place with events scrolling beneath it. It reads plain values off
// engine.Stats and holds no protocol logic.
//
// Every method is safe from any goroutine. On a stream that can't take cursor
// control — a pipe, a file, a dumb terminal — events are appended as plain
// lines, which is what the end-to-end tests read.
type Dashboard struct {
	out  io.Writer
	file *os.File // the underlying file, when out is one, for tty/ioctl queries
	ansi bool     // may this stream be driven with escape sequences?

	// widthFn resolves the width fresh on every frame: a resize is picked up
	// without any signal handling.
	widthFn func() int

	mu      sync.Mutex
	source  func() engine.Stats
	sticky  bool   // a sticky line is currently on screen
	last    string // the last painted frame, to skip unchanged redraws
	closed  bool
	est     estimator
	closeIt sync.Once
}

// New builds a dashboard writing to w. Capability detection happens once, here:
// cursor control only when w is a real terminal and TERM isn't dumb.
func New(w io.Writer) *Dashboard {
	d := &Dashboard{out: w}
	if f, ok := w.(*os.File); ok {
		d.file = f
	}
	d.ansi = canAnimate(d.file)
	d.widthFn = d.termWidth
	return d
}

// canAnimate reports whether a live in-place display is safe on f. A "dumb"
// terminal is a real tty that can't process escapes, so TERM's answer has to
// win over the ioctl's.
func canAnimate(f *os.File) bool {
	if f == nil {
		return false
	}
	if os.Getenv("TERM") == "dumb" {
		return false
	}
	return isTerminal(f)
}

// Animated reports whether this dashboard drives a live in-place display. When
// it's false, events are appended as plain lines and Run returns immediately.
func (d *Dashboard) Animated() bool { return d.ansi }

// Event writes one event line. On an animated stream it erases the sticky line
// first so the event never lands on top of it, prints the event, then puts the
// progress line back underneath. On a plain stream it's just a line.
func (d *Dashboard) Event(text string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.ansi {
		fmt.Fprintln(d.out, text)
		return
	}
	fmt.Fprint(d.out, eraseLine, text, "\n")
	// Put the progress line straight back from the frame already on screen:
	// the stats source would deadlock here, because the engine calls this
	// holding its own lock. The redraw loop refreshes the numbers a tick later.
	if d.last != "" {
		fmt.Fprint(d.out, eraseLine, d.last)
		d.sticky = true
		return
	}
	d.sticky = false
}

// Eventf is Event with printf formatting, matching the engine's Log callback
// shape so the CLI can hand its logger straight to the engine.
func (d *Dashboard) Eventf(format string, args ...any) {
	d.Event(fmt.Sprintf(format, args...))
}

// Track points the display at a stats source. The CLI swaps it on a tracker
// retry and clears it with nil when the download stops, so a finished run
// doesn't leave a stale progress line behind.
func (d *Dashboard) Track(src func() engine.Stats) {
	d.mu.Lock()
	d.source = src
	d.last = ""
	animate := d.ansi
	d.mu.Unlock()

	// Paint the first frame now instead of waiting for the first tick: a
	// download finishing inside one redraw interval would otherwise never show
	// anything. Animated streams only — a pipe must not get escapes.
	if animate && src != nil {
		d.advance(time.Now())
	}
}

// Run redraws at the pinned cadence until ctx is done. It returns at once on a
// stream that can't animate, so callers can start it unconditionally.
func (d *Dashboard) Run(ctx context.Context) {
	if !d.ansi {
		return
	}
	ticker := time.NewTicker(redrawInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			d.advance(now)
		}
	}
}

// Close stops sampling and clears the sticky line, so the shell prompt isn't
// left on a half-drawn row. Idempotent, and safe from the SIGINT path as well
// as a normal return.
func (d *Dashboard) Close() {
	d.closeIt.Do(func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.closed = true
		d.source = nil
		if d.ansi && d.sticky {
			fmt.Fprint(d.out, eraseLine)
			d.sticky = false
			d.last = ""
		}
	})
}

// advance samples the source, folds it into the rate estimate and repaints. A
// frame identical to the one on screen is skipped: the same bytes four times a
// second is pure strobe.
//
// The source is read with no lock held. Calling back into the engine is what
// makes that mandatory: the engine calls Event, which takes this lock, while
// holding its own, so holding d.mu across the call inverts the two locks and
// deadlocks the download — which is exactly what it damn well did before this
// was split. The lock never spans a call out of this package.
func (d *Dashboard) advance(now time.Time) {
	d.mu.Lock()
	src, closed := d.source, d.closed
	d.mu.Unlock()
	if closed || src == nil {
		return
	}

	st := src()

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	d.est.add(now, st.BytesIn)
	frame := Render(st, d.est.value(), d.widthFn())
	if d.sticky && frame == d.last {
		return
	}
	d.paintLocked(frame)
}

// paintLocked erases the current sticky row and writes a new frame into it,
// leaving the cursor at the end of the line. Starting every write with
// erase-line means an overwrite never leaves the tail of a longer frame behind.
func (d *Dashboard) paintLocked(frame string) {
	fmt.Fprint(d.out, eraseLine, frame)
	d.sticky = true
	d.last = frame
}

// termWidth resolves the width for one frame: the ioctl first (it tracks a
// resize), then COLUMNS, then the 80-column default.
func (d *Dashboard) termWidth() int {
	if d.file != nil {
		if cols, ok := terminalWidth(d.file); ok {
			return cols
		}
	}
	if v := os.Getenv("COLUMNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultWidth
}

// estimator turns the engine's cumulative byte counter into a smoothed
// bytes/second rate — the engine reports totals, never rates. Without the
// smoothing the number jumps with every block, and the ETA with it.
type estimator struct {
	last   time.Time
	bytes  int64
	rate   float64
	seeded bool
}

// add folds one sample in. bytes is a cumulative counter, so the instantaneous
// rate is its delta over the elapsed wall time. The first sample just arms the
// estimator — there's no interval to divide by yet.
func (e *estimator) add(now time.Time, bytes int64) {
	if !e.seeded {
		e.last, e.bytes, e.seeded = now, bytes, true
		return
	}
	dt := now.Sub(e.last).Seconds()
	delta := bytes - e.bytes
	e.last, e.bytes = now, bytes
	if dt <= 0 || delta < 0 {
		// A counter that went backwards (a new engine after a tracker retry)
		// would otherwise give a negative rate and a nonsense ETA.
		return
	}
	inst := float64(delta) / dt
	if e.rate == 0 {
		e.rate = inst
		return
	}
	e.rate = rateAlpha*inst + (1-rateAlpha)*e.rate
}

// value is the current smoothed rate in bytes per second; zero means "not
// known yet", which the ETA formatter prints as a placeholder.
func (e *estimator) value() float64 { return e.rate }
