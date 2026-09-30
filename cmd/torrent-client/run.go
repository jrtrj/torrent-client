package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// Exit codes are part of the CLI contract, so they are asserted by tests.
const (
	exitOK    = 0
	exitFatal = 1
	exitUsage = 2
)

// defaultPort is announced to trackers and listened on for peers when -port is
// not given. 6881 is the conventional first port in the BitTorrent range.
const defaultPort = 6881

const usageText = `torrent-client — a from-scratch BitTorrent client.

Usage:
  torrent-client [flags] <torrent-file | magnet-uri> <output-path>

Flags:
  -port int
        TCP port to announce and listen on for incoming peers (default 6881)
  -seed
        keep seeding after the download completes
  -max-down-rate size
        cap download throughput, e.g. 512k or 2M (default: unlimited)
  -max-up-rate size
        cap upload throughput, e.g. 512k or 2M (default: unlimited)
  -h, -help
        print this help and exit

Flags must come before the two positional arguments.

Exit codes:
  0  success
  1  fatal error
  2  usage error
`

// config is a parsed and validated invocation.
type config struct {
	source      string // path to a .torrent file, or a magnet URI
	output      string // where downloaded content is written
	port        int
	seed        bool
	maxDownRate int64 // bytes per second; 0 means unlimited
	maxUpRate   int64 // bytes per second; 0 means unlimited
}

// usageError marks a failure as the caller's fault, which maps to exit code 2.
type usageError struct{ err error }

func (e usageError) Error() string { return e.err.Error() }
func (e usageError) Unwrap() error { return e.err }

// run executes one invocation and reports the process exit code. Keeping it
// separate from main is what makes the CLI contract testable without exec.
func run(args []string, stdout, stderr io.Writer) int {
	cfg, err := parseArgs(args, stdout)
	switch {
	case errors.Is(err, flag.ErrHelp):
		return exitOK
	case err != nil:
		var ue usageError
		if errors.As(err, &ue) {
			fmt.Fprintf(stderr, "torrent-client: %v\n\n%s", ue.err, usageText)
			return exitUsage
		}
		fmt.Fprintf(stderr, "torrent-client: %v\n", err)
		return exitFatal
	}

	// The grammar is valid, so hand off to the download pipeline, which owns
	// the fatal-error reporting from here on.
	return execute(cfg, stdout, stderr)
}

// parseArgs turns raw arguments into a validated config. A help request is
// signalled with flag.ErrHelp; anything the caller got wrong comes back as a
// usageError.
func parseArgs(args []string, usageOut io.Writer) (config, error) {
	var (
		cfg     config
		maxDown string
		maxUp   string
	)

	fs := flag.NewFlagSet("torrent-client", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // usage is rendered by run, not by the flag package
	fs.Usage = func() {}
	fs.IntVar(&cfg.port, "port", defaultPort, "TCP port to announce and listen on")
	fs.BoolVar(&cfg.seed, "seed", false, "keep seeding after the download completes")
	fs.StringVar(&maxDown, "max-down-rate", "", "download cap, e.g. 512k or 2M")
	fs.StringVar(&maxUp, "max-up-rate", "", "upload cap, e.g. 512k or 2M")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(usageOut, usageText)
			return config{}, flag.ErrHelp
		}
		return config{}, usageError{err}
	}

	rest := fs.Args()
	switch {
	case len(rest) < 2:
		return config{}, usageError{errors.New("expected a torrent file or magnet URI and an output path")}
	case len(rest) > 2:
		extra := rest[2]
		// Go's flag package stops at the first non-flag argument, so a flag
		// written after the positionals lands here rather than being parsed.
		if strings.HasPrefix(extra, "-") {
			return config{}, usageError{fmt.Errorf("unexpected extra argument %q: flags must come before the positional arguments", extra)}
		}
		return config{}, usageError{fmt.Errorf("unexpected extra argument %q: expected one torrent file or magnet URI and one output path", extra)}
	}
	cfg.source, cfg.output = rest[0], rest[1]

	if strings.TrimSpace(cfg.source) == "" {
		return config{}, usageError{errors.New("the torrent file or magnet URI must not be empty")}
	}
	if strings.TrimSpace(cfg.output) == "" {
		return config{}, usageError{errors.New("the output path must not be empty")}
	}
	if cfg.port < 1 || cfg.port > 65535 {
		return config{}, usageError{fmt.Errorf("port %d is out of range 1-65535", cfg.port)}
	}

	var err error
	if cfg.maxDownRate, err = parseRate(maxDown); err != nil {
		return config{}, usageError{fmt.Errorf("invalid -max-down-rate: %w", err)}
	}
	if cfg.maxUpRate, err = parseRate(maxUp); err != nil {
		return config{}, usageError{fmt.Errorf("invalid -max-up-rate: %w", err)}
	}
	return cfg, nil
}

// parseRate parses a human-readable size such as "512k" or "2M" into bytes per
// second; an empty string, or any zero size, means unlimited. This is the
// grammar the flag accepts today; the surrounding policy (burst, per-direction
// shaping) is settled by the rate-limiting ticket.
func parseRate(s string) (int64, error) {
	raw := strings.TrimSpace(s)
	if raw == "" {
		return 0, nil
	}

	digits, mult := raw, int64(1)
	switch raw[len(raw)-1] {
	case 'k', 'K':
		mult, digits = 1024, raw[:len(raw)-1]
	case 'm', 'M':
		mult, digits = 1024*1024, raw[:len(raw)-1]
	case 'g', 'G':
		mult, digits = 1024*1024*1024, raw[:len(raw)-1]
	}

	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%q is not a size (want e.g. 512k or 2M)", raw)
	}
	if mult > 1 && n > math.MaxInt64/mult {
		return 0, fmt.Errorf("%q is too large", raw)
	}
	return n * mult, nil
}
