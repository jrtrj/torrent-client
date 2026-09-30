// Package ui renders the live terminal display: the single in-place progress
// line with one-line events beneath it.
//
// It consumes plain values from the engine's statistics and holds no protocol
// logic. The production interface, the redraw cadence, and the fallback for
// terminals that cannot take ANSI are pinned by the dashboard ticket.
//
// Import direction: top of the layering; nothing imports ui.
package ui
