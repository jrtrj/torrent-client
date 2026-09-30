//go:build linux

package ui

import (
	"os"
	"syscall"
	"unsafe"
)

// isTerminal reports whether f is a terminal, via the TCGETS ioctl: it fails
// with ENOTTY on everything that isn't a tty — pipes, regular files, and
// /dev/null, which is a character device and would fool a ModeCharDevice check.
func isTerminal(f *os.File) bool {
	var termios syscall.Termios
	_, _, errno := syscall.Syscall6(syscall.SYS_IOCTL, f.Fd(),
		syscall.TCGETS, uintptr(unsafe.Pointer(&termios)), 0, 0, 0)
	return errno == 0
}

// winsize mirrors struct winsize from <termios.h>: the payload of TIOCGWINSZ,
// in the field order and widths the Linux kernel ABI uses.
type winsize struct {
	rows    uint16
	cols    uint16
	xpixels uint16
	ypixels uint16
}

// terminalWidth asks the tty for its column count. Zero columns means the
// kernel has no size recorded yet (a fresh pty often hasn't), so that's
// reported as unknown and the caller falls back instead of truncating to nothing.
func terminalWidth(f *os.File) (int, bool) {
	var ws winsize
	_, _, errno := syscall.Syscall6(syscall.SYS_IOCTL, f.Fd(),
		syscall.TIOCGWINSZ, uintptr(unsafe.Pointer(&ws)), 0, 0, 0)
	if errno != 0 || ws.cols == 0 {
		return 0, false
	}
	return int(ws.cols), true
}
