// Package ui renders the live terminal display: the single in-place progress
// line with one-line events beneath it.
//
// The shape is fixed by the dashboard ticket:
//
//   - the whole display — sticky line and events — goes to the caller's
//     stream (stderr in the CLI), so it never contaminates stdout;
//   - a single line is redrawn at ~4 Hz, and a frame identical to the one on
//     screen is skipped rather than repainted;
//   - speed and ETA are derived here from the engine's cumulative byte counts,
//     smoothed so the figure does not jitter;
//   - on a stream that cannot take cursor control (a pipe, a file, a dumb
//     terminal) every event is appended as a plain line with no escapes at
//     all, which is the path the end-to-end tests read through.
//
// It consumes plain values from the engine's statistics and holds no protocol
// logic. Import direction: top of the layering; nothing imports ui.
package ui
