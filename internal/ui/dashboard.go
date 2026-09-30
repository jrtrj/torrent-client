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
	// redrawInterval is the ~4 Hz cadence the design pinned. Fast enough to
	// look live, slow enough that the terminal is not asked to repaint for
	// nothing; combined with the skip-if-unchanged check below, a still
	// display costs nothing after the first frame.
	redrawInterval = 250 * time.Millisecond
	// defaultWidth is the fallback when neither ioctl nor COLUMNS answers.
	defaultWidth = 80
	// rateAlpha weights each new instantaneous sample. Sampling at 4 Hz with
	// this weight lets the displayed rate lag the true rate by about a second,
	// which is what stops the number from flickering while still tracking a
	// real change in throughput.
	rateAlpha = 0.35
)

// eraseLine returns the cursor to column 0 and blanks the row.
//
// The dashboard redraws with erase-line rather than save/restore-cursor:
// the sticky line is always the bottom row, and printing an event there can
// scroll the screen, which invalidates a cursor position saved with DECSC —
// the saved row would then point at unrelated text. Erase-line survives
// scrolling because it is re-derived from "wherever the cursor is now" on
// every write.
const eraseLine = "\r\x1b[K"

// Dashboard is the live terminal display: one sticky progress line redrawn in
// place, with event lines scrolling beneath it. It reads plain values from the
// engine's Stats and holds no protocol logic.
//
// Every method is safe to call from any goroutine. On a stream that cannot
// take cursor control (a pipe, a file, a dumb terminal) it degrades to
// appending the event lines exactly as plain text, which is the path the
// end-to-end tests read through.
type Dashboard struct {
	out  io.Writer
	file *os.File // the underlying file, when out is one, for tty/ioctl queries
	ansi bool     // may this stream be driven with escape sequences?

	// widthFn resolves the width freshly on every frame so a resize is picked
	// up without any signal handling.
	widthFn func() int

	mu      sync.Mutex
	source  func() engine.Stats
	sticky  bool   // a sticky line is currently on screen
	last    string // the last painted frame, to skip unchanged redraws
	closed  bool
	est     estimator
	closeIt sync.Once
}

// New builds a dashboard writing to w. Capability detection happens here, once:
// cursor control is used only when w is a real terminal and TERM is not dumb.
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
// terminal is a real tty that cannot process escapes, so TERM's answer wins
// over the ioctl's.
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
// false, events are appended as plain lines and Run is a no-op.
func (d *Dashboard) Animated() bool { return d.ansi }

// Event writes one event line. On an animated stream it erases the sticky
// line first so the event never lands on top of it, prints the event, and
// redraws the sticky line beneath, keeping the progress line as the bottom
// row. On a plain stream it is an ordinary newline-terminated line.
func (d *Dashboard) Event(text string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.ansi {
		fmt.Fprintln(d.out, text)
		return
	}
	fmt.Fprint(d.out, eraseLine, text, "\n")
	// Put the progress line straight back from the frame already on screen.
	// The stats source is deliberately NOT consulted here: the engine calls
	// this while holding its own lock, so calling back into Stats would
	// deadlock, and the redraw loop refreshes the numbers a tick later.
	if d.last != "" {
		fmt.Fprint(d.out, eraseLine, d.last)
		d.sticky = true
		return
	}
	d.sticky = false
}

// Eventf is Event with printf formatting, matching the engine's Log callback
// shape so the CLI can hand the engine its logger directly.
func (d *Dashboard) Eventf(format string, args ...any) {
	d.Event(fmt.Sprintf(format, args...))
}

// Track points the display at a stats source. The CLI replaces it when it
// retries a download with another tracker and clears it with nil once the
// download stops, so a finished run does not leave a stale progress line.
func (d *Dashboard) Track(src func() engine.Stats) {
	d.mu.Lock()
	d.source = src
	d.last = ""
	animate := d.ansi
	d.mu.Unlock()

	// Paint the first frame now rather than waiting for the first tick: a
	// download that finishes inside one redraw interval would otherwise never
	// show the display at all. Only on an animatable stream — a pipe must not
	// receive escape sequences.
	if animate && src != nil {
		d.advance(time.Now())
	}
}

// Run redraws at the pinned cadence until ctx is done. It returns at once on a
// stream that cannot animate, so callers may start it unconditionally.
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

// Close restores the stream: it stops sampling and clears the sticky line so
// the shell prompt is not left on a half-drawn row. It is idempotent and safe
// from the SIGINT path as well as a normal return.
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
// frame identical to the one on screen is skipped: repainting the same bytes
// four times a second is pure strobe.
//
// The source is read with NO lock held. The source is the engine, and the
// engine calls Event (which takes this lock) while holding its own, so holding
// d.mu across the call inverts the two locks and deadlocks the download — which
// is exactly what it did before this was split. The lock is therefore taken
// twice, briefly, and never spans a call out of this package.
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
// erase-line is what makes an overwrite never leave the tail of a longer
// previous frame behind.
func (d *Dashboard) paintLocked(frame string) {
	fmt.Fprint(d.out, eraseLine, frame)
	d.sticky = true
	d.last = frame
}

// termWidth resolves the terminal width for one frame: the ioctl first (it
// tracks a resize), then COLUMNS for terminals and shells that export it, then
// the 80-column default.
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

// estimator turns a cumulative byte counter into a smoothed bytes/second rate.
// The engine reports totals, never rates, so the smoothing lives here; without
// it the number would jump with every block and the ETA with it.
type estimator struct {
	last   time.Time
	bytes  int64
	rate   float64
	seeded bool
}

// add folds one sample in. bytes is a cumulative counter, so the instantaneous
// rate is its delta over the elapsed wall time. The first sample only arms the
// estimator: there is no interval to divide by yet.
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
		// would otherwise produce a negative rate and a nonsense ETA.
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
// known yet", which the ETA formatter renders as a placeholder.
func (e *estimator) value() float64 { return e.rate }
