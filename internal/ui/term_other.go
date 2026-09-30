//go:build !linux

package ui

import "os"

// isTerminal is the portable fallback: a character device is the closest the
// standard library gets to "a terminal" off Linux. It over-reports /dev/null,
// which only costs a few harmless escapes on a stream nobody reads, and on
// Linux the ioctl version is used instead.
func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

// terminalWidth has no portable answer without x/term, and x/term is ruled
// out, so the caller falls through to COLUMNS and then the default.
func terminalWidth(*os.File) (int, bool) { return 0, false }
