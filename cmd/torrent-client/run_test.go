package main

import (
	"math"
	"path/filepath"
	"strings"
	"testing"
)

// runCapture drives the CLI in-process and hands back the exit code plus both
// streams — codes and output are the contract these tests care about.
func runCapture(args ...string) (code int, stdout, stderr string) {
	var out, errOut strings.Builder
	code = run(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestHelpExitsZeroAndPrintsUsageToStdout(t *testing.T) {
	for _, arg := range []string{"-h", "-help", "--help"} {
		code, stdout, stderr := runCapture(arg)
		if code != exitOK {
			t.Errorf("%s: exit = %d, want %d", arg, code, exitOK)
		}
		if !strings.Contains(stdout, "Usage:") {
			t.Errorf("%s: expected usage on stdout, got %q", arg, stdout)
		}
		if stderr != "" {
			t.Errorf("%s: help should not write to stderr, got %q", arg, stderr)
		}
	}
}

func TestMissingArgumentsIsUsageError(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no arguments", nil},
		{"output path missing", []string{"fixture.torrent"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, stdout, stderr := runCapture(tt.args...)
			if code != exitUsage {
				t.Fatalf("exit = %d, want %d", code, exitUsage)
			}
			if stdout != "" {
				t.Errorf("usage error should not write to stdout, got %q", stdout)
			}
			if !strings.Contains(stderr, "Usage:") {
				t.Errorf("expected usage on stderr, got %q", stderr)
			}
		})
	}
}

func TestTooManyArgumentsIsUsageError(t *testing.T) {
	code, _, stderr := runCapture("fixture.torrent", "out", "extra")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr, "extra") {
		t.Errorf("expected the offending argument in the message, got %q", stderr)
	}
	// "extra" isn't a flag, so a flag-ordering hint here would be a lie.
	if strings.Contains(stderr, "before the positional") {
		t.Errorf("a non-flag extra argument should not blame flag ordering, got %q", stderr)
	}
}

// Writing a flag after the positionals is the classic own-goal: flag stops
// parsing at the first non-flag argument.
func TestFlagAfterPositionalsIsUsageError(t *testing.T) {
	code, _, stderr := runCapture("fixture.torrent", "out", "-seed")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr, "before the positional") {
		t.Errorf("expected a flag-order hint, got %q", stderr)
	}
}

func TestUnknownFlagIsUsageError(t *testing.T) {
	if code, _, _ := runCapture("-nope", "fixture.torrent", "out"); code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
}

func TestEmptyPositionalsAreUsageErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"empty source", []string{"", "out"}},
		{"blank source", []string{"   ", "out"}},
		{"empty output", []string{"fixture.torrent", ""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if code, _, _ := runCapture(tt.args...); code != exitUsage {
				t.Fatalf("exit = %d, want %d", code, exitUsage)
			}
		})
	}
}

func TestPortRange(t *testing.T) {
	for _, port := range []string{"0", "65536", "-1", "abc", "6881x"} {
		t.Run("port="+port, func(t *testing.T) {
			if code, _, _ := runCapture("-port", port, "fixture.torrent", "out"); code != exitUsage {
				t.Fatalf("exit = %d, want %d", code, exitUsage)
			}
		})
	}
	for _, port := range []string{"1", "6881", "65535"} {
		t.Run("port="+port, func(t *testing.T) {
			// Grammar's fine, the pipeline isn't: fatal, not usage.
			if code, _, _ := runCapture("-port", port, "fixture.torrent", "out"); code != exitFatal {
				t.Fatalf("exit = %d, want %d", code, exitFatal)
			}
		})
	}
}

func TestBadRateIsUsageError(t *testing.T) {
	for _, flagName := range []string{"-max-down-rate", "-max-up-rate"} {
		for _, value := range []string{"fast", "1.5M", "-5", "k", "2MB"} {
			t.Run(flagName+"="+value, func(t *testing.T) {
				if code, _, _ := runCapture(flagName, value, "fixture.torrent", "out"); code != exitUsage {
					t.Fatalf("exit = %d, want %d", code, exitUsage)
				}
			})
		}
	}
}

// Well-formed args, unopenable file: a runtime failure, so exit 1, not usage.
func TestWellFormedInvocationWithMissingTorrentIsFatal(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "fixture.torrent")
	code, stdout, stderr := runCapture(missing, "out/")
	if code != exitFatal {
		t.Fatalf("exit = %d, want %d", code, exitFatal)
	}
	if stdout != "" {
		t.Errorf("nothing should reach stdout, got %q", stdout)
	}
	if !strings.Contains(stderr, "fixture.torrent") {
		t.Errorf("expected the offending path in the message, got %q", stderr)
	}
	if strings.Contains(stderr, "Usage:") {
		t.Errorf("a runtime failure should not print usage, got %q", stderr)
	}
}

func TestFlagsBeforePositionalsAreAccepted(t *testing.T) {
	code, _, stderr := runCapture("-port", "6882", "-seed", "-max-down-rate", "512k", "-max-up-rate", "2M", "fixture.torrent", "out/")
	if code != exitFatal {
		t.Fatalf("exit = %d, want %d (valid grammar, unimplemented pipeline); stderr %q", code, exitFatal, stderr)
	}
	if strings.Contains(stderr, "Usage:") {
		t.Errorf("valid grammar should not print usage, got %q", stderr)
	}
}

func TestParseRate(t *testing.T) {
	tests := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{in: "", want: 0},
		{in: "0", want: 0},
		{in: "1024", want: 1024},
		{in: "512k", want: 512 * 1024},
		{in: "2M", want: 2 * 1024 * 1024},
		{in: "1G", want: 1024 * 1024 * 1024},
		{in: "  2M  ", want: 2 * 1024 * 1024},
		{in: "k", wantErr: true},
		{in: "M", wantErr: true},
		{in: "1.5M", wantErr: true},
		{in: "-5", wantErr: true},
		{in: "2MB", wantErr: true},
		{in: "abc", wantErr: true},
		{in: "99999999999999999999", wantErr: true},
		{in: "9223372036854775807G", wantErr: true}, // the 1G multiplier overflows int64
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseRate(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseRate(%q) = %d, want an error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRate(%q) unexpected error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("parseRate(%q) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseRateAcceptsMaxInt64(t *testing.T) {
	// MaxInt64 with no suffix: exactly representable.
	got, err := parseRate("9223372036854775807")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != math.MaxInt64 {
		t.Fatalf("got %d, want %d", got, int64(math.MaxInt64))
	}
}
