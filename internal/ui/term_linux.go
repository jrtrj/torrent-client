//go:build linux

package ui

import (
	"os"
	"syscall"
	"unsafe"
)

// isTerminal reports whether f is a terminal. The ioctl is TCGETS: it fails
// with ENOTTY on everything that is not a tty — a pipe, a regular file, and
// also /dev/null, which is a character device. That last distinction is why
// the trivial os.File.Stat().ModeCharDevice check is not good enough here:
// it would classify /dev/null as a terminal and emit escapes into it.
func isTerminal(f *os.File) bool {
	var termios syscall.Termios
	_, _, errno := syscall.Syscall6(syscall.SYS_IOCTL, f.Fd(),
		syscall.TCGETS, uintptr(unsafe.Pointer(&termios)), 0, 0, 0)
	return errno == 0
}

// winsize mirrors struct winsize from <termios.h>, the payload of
// TIOCGWINSZ. Field order and widths are the kernel ABI on Linux.
type winsize struct {
	rows    uint16
	cols    uint16
	xpixels uint16
	ypixels uint16
}

// terminalWidth asks the terminal how many columns it has. A zero column
// count means the kernel has no size recorded (common for a fresh pty), which
// is reported as "unknown" so the caller can fall back rather than truncate
// the display to nothing.
func terminalWidth(f *os.File) (int, bool) {
	var ws winsize
	_, _, errno := syscall.Syscall6(syscall.SYS_IOCTL, f.Fd(),
		syscall.TIOCGWINSZ, uintptr(unsafe.Pointer(&ws)), 0, 0, 0)
	if errno != 0 || ws.cols == 0 {
		return 0, false
	}
	return int(ws.cols), true
}
