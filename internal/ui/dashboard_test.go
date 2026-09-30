package ui

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"torrent-client/internal/engine"
)

// A pipe isn't a terminal, so the dashboard takes the plain path: events become
// ordinary lines and no escape sequence ever reaches the stream. This is also
// the path the end-to-end tests read, which is why it has to stay plain.
func TestPlainStreamIsNotAnimatedAndWritesNoEscapes(t *testing.T) {
	var buf bytes.Buffer
	d := New(&buf)
	if d.Animated() {
		t.Fatal("a bytes.Buffer must not be treated as a terminal")
	}

	d.Event("peer connected")
	d.Eventf("piece %d verified", 3)

	got := buf.String()
	if strings.ContainsRune(got, 0x1b) {
		t.Fatalf("plain stream received escape sequences: %q", got)
	}
	if want := "peer connected\npiece 3 verified\n"; got != want {
		t.Fatalf("plain stream output = %q, want %q", got, want)
	}
}

func TestRegularFileIsNotAnimated(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "progress.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if New(f).Animated() {
		t.Fatal("a regular file must not be treated as a terminal")
	}
}

// /dev/null is a character device, so a naive ModeCharDevice check would call it
// a terminal and drive it with escapes. The ioctl has to reject it.
func TestCharacterDeviceIsNotATerminal(t *testing.T) {
	f, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if New(f).Animated() {
		t.Fatal("/dev/null must not be treated as a terminal")
	}
}

// Callers start Run unconditionally, so on a stream that can't animate it has to
// return rather than block until the context ends.
func TestRunReturnsImmediatelyOnAPlainStream(t *testing.T) {
	var buf bytes.Buffer
	d := New(&buf)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run blocked on a plain stream; it must return at once")
	}
}

// progressSource is a fixed snapshot: a frame only changes if the renderer or
// the rate estimator changes it.
func progressSource(st engine.Stats) func() engine.Stats {
	return func() engine.Stats { return st }
}

func TestTermWidthFallsBackFromColumnsToTheDefault(t *testing.T) {
	d := New(&bytes.Buffer{}) // not a *os.File, so no ioctl is available

	t.Setenv("COLUMNS", "57")
	if got := d.termWidth(); got != 57 {
		t.Fatalf("termWidth() = %d, want 57 from COLUMNS", got)
	}

	t.Setenv("COLUMNS", "not-a-number")
	if got := d.termWidth(); got != defaultWidth {
		t.Fatalf("termWidth() = %d, want the %d default", got, defaultWidth)
	}

	t.Setenv("COLUMNS", "")
	if got := d.termWidth(); got != defaultWidth {
		t.Fatalf("termWidth() = %d, want the %d default", got, defaultWidth)
	}
}

// The animated path needs a terminal, which a test doesn't have. Flipping the
// capability flag the constructor would have set on a real tty exercises the
// same code without a pty.
func TestAnimatedPathPaintsInPlaceAndLeavesACleanLine(t *testing.T) {
	var buf bytes.Buffer
	d := New(&buf)
	d.ansi = true
	d.widthFn = func() int { return 60 }
	d.Track(progressSource(engine.Stats{
		Pieces: 2, PiecesDone: 1, BytesTotal: 200, BytesDone: 100, BytesIn: 100,
		PeersActive: 1, Peers: make([]engine.PeerStat, 1),
	}))

	now := time.Now()
	d.advance(now)

	if !strings.Contains(buf.String(), eraseLine) {
		t.Fatalf("animated path wrote no erase-line sequence: %q", buf.String())
	}
	if strings.Contains(buf.String(), "\n") {
		t.Fatalf("the sticky line must not emit newlines: %q", buf.String())
	}

	d.Close()
	if got := buf.String(); !strings.HasSuffix(got, eraseLine) {
		t.Fatalf("Close left the sticky line on screen: %q", got)
	}
}

// Repainting an identical frame four times a second is pure strobe.
func TestUnchangedFrameIsNotRepainted(t *testing.T) {
	var buf bytes.Buffer
	d := New(&buf)
	d.ansi = true
	d.widthFn = func() int { return 80 }
	d.Track(progressSource(engine.Stats{Pieces: 1, BytesTotal: 100, BytesIn: 7}))

	now := time.Now()
	d.advance(now)
	first := buf.Len()
	d.advance(now.Add(redrawInterval))
	second := buf.Len()

	if first == 0 {
		t.Fatal("the first frame painted nothing")
	}
	if second != first {
		t.Fatalf("an unchanged frame was repainted: %d -> %d bytes", first, second)
	}
}

// An event must erase the sticky line, print itself, and put the line back
// underneath, or the event lands on top of the progress display.
func TestEventPrintsBeneathTheStickyLineThenRestoresIt(t *testing.T) {
	var buf bytes.Buffer
	d := New(&buf)
	d.ansi = true
	d.widthFn = func() int { return 80 }
	d.Track(progressSource(engine.Stats{Pieces: 1, BytesTotal: 100, BytesIn: 7}))

	d.advance(time.Now())
	buf.Reset()

	d.Event("peer connected")

	got := buf.String()
	if !strings.Contains(got, eraseLine+"peer connected\n") {
		t.Fatalf("event was not printed under an erased sticky line: %q", got)
	}
	if strings.HasSuffix(got, "\n") {
		t.Fatalf("the sticky line was not restored after the event: %q", got)
	}
}

// Close runs from the normal return path and from the SIGINT path, so it has to
// be safe to call twice.
func TestCloseIsIdempotent(t *testing.T) {
	var buf bytes.Buffer
	d := New(&buf)
	d.ansi = true
	d.widthFn = func() int { return 80 }
	d.Track(progressSource(engine.Stats{Pieces: 1, BytesTotal: 100}))

	d.advance(time.Now())

	d.Close()
	after := buf.Len()
	d.Close()

	if buf.Len() != after {
		t.Fatalf("second Close wrote again: %d -> %d bytes", after, buf.Len())
	}
}

// A tracker retry swaps the stats source; a closed dashboard must not paint.
func TestClosedDashboardStopsPainting(t *testing.T) {
	var buf bytes.Buffer
	d := New(&buf)
	d.ansi = true
	d.widthFn = func() int { return 80 }
	d.Track(progressSource(engine.Stats{Pieces: 1, BytesTotal: 100, BytesIn: 7}))

	d.advance(time.Now())
	d.Close()

	after := buf.Len()
	d.advance(time.Now().Add(time.Second))

	if buf.Len() != after {
		t.Fatalf("a closed dashboard painted again: %d -> %d bytes", after, buf.Len())
	}
}

// The deadlock regression. The engine invokes its log callback from under its
// own lock, and the redraw loop takes the dashboard's lock then calls back into
// engine.Stats: hold one lock across the other's call and they invert, hanging a
// real download on any animated terminal. This reproduces the ordering and
// fails by timing out if the inversion comes back.
func TestEventUnderTheSourceLockDoesNotDeadlock(t *testing.T) {
	var engineMu sync.Mutex
	var buf bytes.Buffer

	d := New(&buf)
	d.ansi = true
	d.widthFn = func() int { return 60 }
	d.Track(func() engine.Stats {
		engineMu.Lock()
		defer engineMu.Unlock()
		return engine.Stats{Pieces: 1, BytesTotal: 100, BytesIn: 7}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)
	defer d.Close()

	// The engine side: it holds its lock while it logs.
	go func() {
		for i := 0; i < 2000; i++ {
			engineMu.Lock()
			d.Eventf("engine: piece %d verified", i+1)
			engineMu.Unlock()
		}
	}()

	// The redraw side, driven directly rather than through the ticker, so the
	// two lock orderings actually collide instead of racing the 250ms clock.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			d.advance(time.Now())
		}
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("deadlock: the redraw loop and the engine's log callback inverted their locks")
	}
}
