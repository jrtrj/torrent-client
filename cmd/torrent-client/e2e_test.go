package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"torrent-client/internal/bencode"
	"torrent-client/internal/metainfo"
	"torrent-client/internal/state"
	"torrent-client/internal/wire"
)

// TestEndToEndLoopbackDownload is the walking-skeleton proof: it builds the
// three binaries, runs a dev tracker and a seeder against a fixture over
// loopback, downloads the fixture with the real CLI, and requires the result
// to be byte-identical to the source. No network access beyond 127.0.0.1.
func TestEndToEndLoopbackDownload(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end download skipped in -short mode")
	}

	root := moduleRoot(t)
	binDir := t.TempDir()
	devtrackerBin := buildBinary(t, root, binDir, "./cmd/devtracker")
	seedBin := buildBinary(t, root, binDir, "./cmd/seed")
	clientBin := buildBinary(t, root, binDir, "./cmd/torrent-client")

	// 1. Dev tracker on an OS-assigned port.
	devtracker := startProcess(t, devtrackerBin, "-addr", "127.0.0.1:0", "-interval", "2")
	line := devtracker.waitForLine(t, "listening on", 20*time.Second)
	addr := strings.TrimSpace(line[strings.Index(line, "listening on ")+len("listening on "):])
	announceURL := "http://" + addr + "/announce"

	// 2. Fixture: 700 KiB across three 256 KiB pieces.
	const pieceLength = 256 * 1024
	payload := make([]byte, 700*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	workDir := t.TempDir()
	srcPath := filepath.Join(workDir, "payload.bin")
	if err := os.WriteFile(srcPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	torrentPath := filepath.Join(workDir, "payload.torrent")
	infoBytes := writeTorrent(t, torrentPath, announceURL, "payload.bin", pieceLength, payload)

	// The client's captured info-hash must equal an independent SHA-1 of the
	// raw info bytes. Peers, the seeder, and the tracker would all reject a
	// mismatch, but assert it directly so a failure says which layer broke.
	meta, err := metainfo.Load(torrentPath)
	if err != nil {
		t.Fatalf("metainfo.Load: %v", err)
	}
	if want := sha1.Sum(infoBytes); meta.InfoHash != want {
		t.Fatalf("info-hash = %x, want %x", meta.InfoHash, want)
	}
	if meta.PieceCount() != 3 {
		t.Fatalf("fixture has %d pieces, want 3", meta.PieceCount())
	}

	// 3. Seeder for the fixture.
	seeder := startProcess(t, seedBin, torrentPath, srcPath)
	seeder.waitForLine(t, "seed: ready", 20*time.Second)

	// 4. The real CLI downloads it.
	outPath := filepath.Join(t.TempDir(), "downloaded.bin")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := exec.CommandContext(ctx, clientBin, torrentPath, outPath)
	var stdout, stderr bytes.Buffer
	client.Stdout = &stdout
	client.Stderr = &stderr
	if err := client.Run(); err != nil {
		t.Fatalf("client failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	t.Logf("client stderr:\n%s", stderr.String())

	// 5. Byte-identical.
	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("downloaded file differs: got %d bytes (sha256 %x), want %d bytes (sha256 %x)",
			len(got), sha256.Sum256(got), len(payload), sha256.Sum256(payload))
	}
	t.Logf("source     sha256 %x", sha256.Sum256(payload))
	t.Logf("downloaded sha256 %x", sha256.Sum256(got))

	if after, err := metainfo.Load(torrentPath); err != nil || after.InfoHash != meta.InfoHash {
		t.Fatalf("torrent changed during the test: %v", err)
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no go.mod found above %s", dir)
		}
		dir = parent
	}
}

func buildBinary(t *testing.T, root, outDir, pkg string) string {
	t.Helper()
	out := filepath.Join(outDir, filepath.Base(pkg))
	cmd := exec.Command("go", "build", "-o", out, pkg)
	cmd.Dir = root
	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build %s: %v\n%s", pkg, err, combined)
	}
	return out
}

// process is a started helper binary whose stdout is streamed line by line.
type process struct {
	cmd   *exec.Cmd
	lines chan string
}

func startProcess(t *testing.T, name string, args ...string) *process {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	lines := make(chan string, 256)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return &process{cmd: cmd, lines: lines}
}

func (p *process) waitForLine(t *testing.T, substr string, timeout time.Duration) string {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case line, ok := <-p.lines:
			if !ok {
				t.Fatalf("%s exited before printing %q", p.cmd.Path, substr)
			}
			if strings.Contains(line, substr) {
				return line
			}
		case <-deadline:
			t.Fatalf("timed out after %s waiting for %q from %s", timeout, substr, p.cmd.Path)
		}
	}
}

// fixtureInfo is the single-file info dictionary the test fixture uses.
type fixtureInfo struct {
	Length      int64  `bencode:"length"`
	Name        string `bencode:"name"`
	PieceLength int64  `bencode:"piece length"`
	Pieces      []byte `bencode:"pieces"`
}

// writeTorrent builds a single-file .torrent for data and returns the raw
// info-dictionary bytes, so the caller can hash them independently. The info
// bytes are embedded verbatim as a RawMessage: re-encoding them must never
// happen, which is the whole point of the raw capture.
func writeTorrent(t *testing.T, path, announce, name string, pieceLength int64, data []byte) []byte {
	t.Helper()

	var pieces []byte
	for off := 0; off < len(data); off += int(pieceLength) {
		end := off + int(pieceLength)
		if end > len(data) {
			end = len(data)
		}
		sum := sha1.Sum(data[off:end])
		pieces = append(pieces, sum[:]...)
	}

	infoBytes := marshalBencode(t, fixtureInfo{
		Length:      int64(len(data)),
		Name:        name,
		PieceLength: pieceLength,
		Pieces:      pieces,
	})
	torrent := marshalBencode(t, struct {
		Announce string             `bencode:"announce"`
		Info     bencode.RawMessage `bencode:"info"`
	}{Announce: announce, Info: infoBytes})

	if err := os.WriteFile(path, torrent, 0o644); err != nil {
		t.Fatalf("write torrent: %v", err)
	}
	return infoBytes
}

func marshalBencode(t *testing.T, v any) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := bencode.Marshal(&buf, v); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return buf.Bytes()
}

// TestEndToEndConcurrentPeers proves the client spreads one download across
// more than one seeder at the same time. Both seeders log every block they
// serve with a timestamp, so the test can show the two serving windows
// overlap; the client's own statistics report the peak number of peers with
// work in the request pipeline. No block may be fetched twice, which is what a
// duplicated-work scheduler bug would produce.
func TestEndToEndConcurrentPeers(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end download skipped in -short mode")
	}

	root := moduleRoot(t)
	binDir := t.TempDir()
	devtrackerBin := buildBinary(t, root, binDir, "./cmd/devtracker")
	seedBin := buildBinary(t, root, binDir, "./cmd/seed")
	clientBin := buildBinary(t, root, binDir, "./cmd/torrent-client")

	devtracker := startProcess(t, devtrackerBin, "-addr", "127.0.0.1:0", "-interval", "2")
	line := devtracker.waitForLine(t, "listening on", 20*time.Second)
	addr := strings.TrimSpace(line[strings.Index(line, "listening on ")+len("listening on "):])
	announceURL := "http://" + addr + "/announce"

	// Three 256 KiB pieces, 48 blocks of 16 KiB in total.
	const pieceLength = 256 * 1024
	payload := make([]byte, 3*pieceLength)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	workDir := t.TempDir()
	srcPath := filepath.Join(workDir, "payload.bin")
	if err := os.WriteFile(srcPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	torrentPath := filepath.Join(workDir, "payload.torrent")
	writeTorrent(t, torrentPath, announceURL, "payload.bin", pieceLength, payload)

	meta, err := metainfo.Load(torrentPath)
	if err != nil {
		t.Fatalf("metainfo.Load: %v", err)
	}
	wantBlocks := 0
	for i := 0; i < meta.PieceCount(); i++ {
		wantBlocks += int((meta.PieceSize(i) + wire.BlockSize - 1) / wire.BlockSize)
	}

	// A per-block delay keeps requests outstanding on both connections, so a
	// client that used one seeder at a time would show two disjoint windows.
	const serveDelay = 15 * time.Millisecond
	seederA := startRecorded(t, seedBin, "-serve-delay", serveDelay.String(), torrentPath, srcPath)
	seederB := startRecorded(t, seedBin, "-serve-delay", serveDelay.String(), torrentPath, srcPath)
	seederA.waitFor(t, "seed: ready", 20*time.Second)
	seederB.waitFor(t, "seed: ready", 20*time.Second)

	outPath := filepath.Join(t.TempDir(), "downloaded.bin")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := exec.CommandContext(ctx, clientBin, torrentPath, outPath)
	var stdout, stderr bytes.Buffer
	client.Stdout = &stdout
	client.Stderr = &stderr
	if err := client.Run(); err != nil {
		t.Fatalf("client failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	t.Logf("client stderr:\n%s", stderr.String())

	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("downloaded file differs: got %d bytes (sha256 %x), want %d bytes (sha256 %x)",
			len(got), sha256.Sum256(got), len(payload), sha256.Sum256(payload))
	}

	// 1. Both seeders served part of the download.
	aServed := seederA.parseServed(t)
	bServed := seederB.parseServed(t)
	if len(aServed) == 0 || len(bServed) == 0 {
		t.Fatalf("both seeders must serve blocks: A served %d, B served %d", len(aServed), len(bServed))
	}
	t.Logf("seeder A served %d blocks, seeder B served %d blocks", len(aServed), len(bServed))

	// 2. No block was fetched twice, and the swarm covered every block.
	counts := make(map[[2]uint32]int)
	for _, b := range append(append([]servedBlock{}, aServed...), bServed...) {
		counts[b.key]++
	}
	if len(counts) != wantBlocks {
		t.Fatalf("the swarm served %d distinct blocks, want %d", len(counts), wantBlocks)
	}
	for key, n := range counts {
		if n != 1 {
			t.Fatalf("block piece=%d begin=%d was served %d times", key[0], key[1], n)
		}
	}

	// 3. The serving windows overlap, i.e. both seeders were busy at once.
	aFirst, aLast := seederA.span(t, "seed: served")
	bFirst, bLast := seederB.span(t, "seed: served")
	overlapStart := aFirst
	if bFirst.After(overlapStart) {
		overlapStart = bFirst
	}
	overlapEnd := aLast
	if bLast.Before(overlapEnd) {
		overlapEnd = bLast
	}
	if overlapStart.After(overlapEnd) {
		t.Fatalf("seeding windows did not overlap: A %s..%s, B %s..%s",
			aFirst.Format(time.StampMilli), aLast.Format(time.StampMilli),
			bFirst.Format(time.StampMilli), bLast.Format(time.StampMilli))
	}
	t.Logf("serving windows overlap for %s", overlapEnd.Sub(overlapStart))

	// 4. The client's own statistics agree that more than one peer worked at
	// the same time.
	if used := grepInt(t, stderr.String(), "peers used"); used < 2 {
		t.Fatalf("client reported %d peer(s) used, want at least 2", used)
	}
	if peak := grepInt(t, stderr.String(), "max active peers"); peak < 2 {
		t.Fatalf("client reported %d peer(s) active at peak, want at least 2", peak)
	}
}

// servedBlock is one block a seeder answered.
type servedBlock struct {
	key [2]uint32 // piece, begin
}

type stampedLine struct {
	at   time.Time
	text string
}

// lineLog keeps a timestamped copy of a helper process's stdout, so the test
// can reason about when the process was doing work.
type lineLog struct {
	mu    sync.Mutex
	lines []stampedLine
}

func (l *lineLog) add(text string) {
	l.mu.Lock()
	l.lines = append(l.lines, stampedLine{at: time.Now(), text: text})
	l.mu.Unlock()
}

func (l *lineLog) snapshot() []stampedLine {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]stampedLine(nil), l.lines...)
}

// recordedProcess is startProcess plus a drain that timestamps every line.
// Use its waitFor/span rather than waitForLine: the drain consumes the channel.
type recordedProcess struct {
	*process
	log *lineLog
}

func startRecorded(t *testing.T, name string, args ...string) *recordedProcess {
	t.Helper()
	p := startProcess(t, name, args...)
	rp := &recordedProcess{process: p, log: &lineLog{}}
	go func() {
		for line := range p.lines {
			rp.log.add(line)
		}
	}()
	return rp
}

func (r *recordedProcess) waitFor(t *testing.T, substr string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, l := range r.log.snapshot() {
			if strings.Contains(l.text, substr) {
				return l.text
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %q from %s", timeout, substr, r.cmd.Path)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (r *recordedProcess) parseServed(t *testing.T) []servedBlock {
	t.Helper()
	var out []servedBlock
	for _, l := range r.log.snapshot() {
		if !strings.HasPrefix(l.text, "seed: served ") {
			continue
		}
		var piece int
		var begin, length uint32
		if _, err := fmt.Sscanf(l.text, "seed: served piece=%d begin=%d length=%d", &piece, &begin, &length); err != nil {
			t.Fatalf("parse %q: %v", l.text, err)
		}
		if length == 0 || length > wire.BlockSize {
			t.Fatalf("%s served a %d byte block", r.cmd.Path, length)
		}
		out = append(out, servedBlock{key: [2]uint32{uint32(piece), begin}})
	}
	return out
}

// span returns the first and last time a line matching substr was seen.
func (r *recordedProcess) span(t *testing.T, substr string) (time.Time, time.Time) {
	t.Helper()
	var first, last time.Time
	for _, l := range r.log.snapshot() {
		if !strings.Contains(l.text, substr) {
			continue
		}
		if first.IsZero() {
			first = l.at
		}
		last = l.at
	}
	if first.IsZero() {
		t.Fatalf("no line matching %q from %s", substr, r.cmd.Path)
	}
	return first, last
}

// grepInt reads the first decimal number that follows label in text.
func grepInt(t *testing.T, text, label string) int {
	t.Helper()
	i := strings.Index(text, label)
	if i < 0 {
		t.Fatalf("%q not found in the client's output:\n%s", label, text)
	}
	rest := strings.TrimLeft(text[i+len(label):], " :=,")
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	if end == 0 {
		t.Fatalf("no number after %q", label)
	}
	n, err := strconv.Atoi(rest[:end])
	if err != nil {
		t.Fatalf("parse %q: %v", label, err)
	}
	return n
}

// freePort reserves a TCP port and releases it, so the next binder can use the
// number. It is how the seeding client is given the concrete port it must
// announce; the CLI rejects 0 as a usage error, so a real port is required.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// clientProc is the real CLI left running in the background with its stderr
// streamed, so a test can assert on what it logged while it was alive.
type clientProc struct {
	cmd   *exec.Cmd
	lines *lineLog
	drain chan struct{}
	once  sync.Once
	werr  error
}

func startClient(t *testing.T, bin string, args ...string) *clientProc {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Stdout = io.Discard
	pipe, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", bin, err)
	}
	p := &clientProc{cmd: cmd, lines: &lineLog{}, drain: make(chan struct{})}
	go func() {
		defer close(p.drain)
		scanner := bufio.NewScanner(pipe)
		for scanner.Scan() {
			p.lines.add(scanner.Text())
		}
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-p.drain:
		case <-time.After(5 * time.Second):
		}
		_ = p.wait()
	})
	return p
}

func (p *clientProc) waitFor(t *testing.T, substr string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, l := range p.lines.snapshot() {
			if strings.Contains(l.text, substr) {
				return l.text
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %q from %s;\nlog:\n%s",
				timeout, substr, p.cmd.Path, p.logText())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// wait blocks until the drain has seen EOF (the process has exited) and then
// reaps it, in the order StderrPipe requires.
func (p *clientProc) wait() error {
	p.once.Do(func() {
		<-p.drain
		p.werr = p.cmd.Wait()
	})
	return p.werr
}

// waitExit requires the process to exit within timeout, then reaps it.
func (p *clientProc) waitExit(t *testing.T, timeout time.Duration) error {
	t.Helper()
	select {
	case <-p.drain:
	case <-time.After(timeout):
		t.Fatalf("client %s did not exit within %s;\nlog:\n%s", p.cmd.Path, timeout, p.logText())
	}
	return p.wait()
}

// countLines is how many streamed lines contain substr.
func (p *clientProc) countLines(substr string) int {
	n := 0
	for _, l := range p.lines.snapshot() {
		if strings.Contains(l.text, substr) {
			n++
		}
	}
	return n
}

func (p *clientProc) logText() string {
	var b strings.Builder
	for _, l := range p.lines.snapshot() {
		b.WriteString(l.text)
		b.WriteString("\n")
	}
	return b.String()
}

// assertFileEquals requires path to hold exactly want.
func assertFileEquals(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s differs: got %d bytes (sha256 %x), want %d bytes (sha256 %x)",
			path, len(got), sha256.Sum256(got), len(want), sha256.Sum256(want))
	}
}

// trackerMaxUploaded is the highest uploaded= counter the dev tracker has
// logged so far; the dev tracker prints one line per announce.
func trackerMaxUploaded(p *recordedProcess) int64 {
	var max int64
	for _, l := range p.log.snapshot() {
		i := strings.Index(l.text, "uploaded=")
		if i < 0 {
			continue
		}
		rest := l.text[i+len("uploaded="):]
		end := 0
		for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
			end++
		}
		if end == 0 {
			continue
		}
		if n, err := strconv.ParseInt(rest[:end], 10, 64); err == nil && n > max {
			max = n
		}
	}
	return max
}

// TestEndToEndSeedingServesSecondClient is the upload proof. Client A starts
// with -seed and downloads the fixture, keeping its listener up. The fixture
// seeder is then stopped, so the only source left is A. A second client B
// downloads the same torrent and must end up byte-identical, which is only
// possible if A served it. The test also requires A to log served blocks, the
// first client to have stayed alive after completing, and the dev tracker to
// see A's uploaded counter move.
func TestEndToEndSeedingServesSecondClient(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end seeding skipped in -short mode")
	}

	root := moduleRoot(t)
	binDir := t.TempDir()
	devtrackerBin := buildBinary(t, root, binDir, "./cmd/devtracker")
	seedBin := buildBinary(t, root, binDir, "./cmd/seed")
	clientBin := buildBinary(t, root, binDir, "./cmd/torrent-client")

	devtracker := startRecorded(t, devtrackerBin, "-addr", "127.0.0.1:0", "-interval", "1")
	line := devtracker.waitFor(t, "listening on", 20*time.Second)
	addr := strings.TrimSpace(line[strings.Index(line, "listening on ")+len("listening on "):])
	announceURL := "http://" + addr + "/announce"

	const pieceLength = 64 * 1024
	payload := make([]byte, 3*pieceLength)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	workDir := t.TempDir()
	srcPath := filepath.Join(workDir, "payload.bin")
	if err := os.WriteFile(srcPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	torrentPath := filepath.Join(workDir, "payload.torrent")
	infoBytes := writeTorrent(t, torrentPath, announceURL, "payload.bin", pieceLength, payload)

	meta, err := metainfo.Load(torrentPath)
	if err != nil {
		t.Fatalf("metainfo.Load: %v", err)
	}
	if want := sha1.Sum(infoBytes); meta.InfoHash != want {
		t.Fatalf("info-hash = %x, want %x", meta.InfoHash, want)
	}

	// The fixture seeder. A per-block delay widens A's download window, so A's
	// listener is genuinely up while A is still fetching.
	fixture := startRecorded(t, seedBin, "-serve-delay", "10ms", torrentPath, srcPath)
	fixture.waitFor(t, "seed: ready", 20*time.Second)

	// A downloads and then keeps seeding. It must bind the port it announces,
	// so it is given a concrete free port (the CLI rejects -port 0).
	port := freePort(t)
	outA := filepath.Join(t.TempDir(), "A.bin")
	a := startClient(t, clientBin, "-seed", "-port", strconv.Itoa(port), torrentPath, outA)
	a.waitFor(t, "engine: seeding", 90*time.Second)
	assertFileEquals(t, outA, payload)

	// Stop the fixture so B has nothing but A to fetch from.
	if err := fixture.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("stop fixture seeder: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// B downloads the same torrent. Without A's upload path this cannot work.
	outB := filepath.Join(t.TempDir(), "B.bin")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	bc := exec.CommandContext(ctx, clientBin, torrentPath, outB)
	var bStderr bytes.Buffer
	bc.Stderr = &bStderr
	if err := bc.Run(); err != nil {
		t.Fatalf("second client failed: %v\nstderr:\n%s", err, bStderr.String())
	}
	assertFileEquals(t, outB, payload)
	t.Logf("second client stderr:\n%s", bStderr.String())

	// A actually served blocks, not just held them.
	if served := a.countLines("served piece="); served == 0 {
		t.Fatalf("the seeding client served no blocks;\nlog:\n%s", a.logText())
	} else {
		t.Logf("seeding client served %d blocks", served)
	}

	// The tracker saw A's uploaded counter move: poll because the client
	// re-announces on the tracker's interval.
	deadline := time.Now().Add(20 * time.Second)
	var uploaded int64
	for time.Now().Before(deadline) {
		if uploaded = trackerMaxUploaded(devtracker); uploaded > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if uploaded <= 0 {
		t.Fatalf("the tracker never saw uploaded > 0 from the serving client;\ndev tracker log:\n%s",
			devtracker.logText())
	}
	t.Logf("tracker saw uploaded=%d bytes from the swarm", uploaded)

	// Ctrl-C while seeding is a clean exit, not a fatal one, and the client
	// announces stopped on the way out.
	if err := a.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("interrupt seeding client: %v", err)
	}
	if err := a.waitExit(t, 15*time.Second); err != nil {
		t.Fatalf("seeding client exited with %v, want 0;\nlog:\n%s", err, a.logText())
	}
	if a.countLines("seeding stopped") == 0 {
		t.Fatalf("seeding client did not log a clean stop;\nlog:\n%s", a.logText())
	}
}

func (r *recordedProcess) logText() string {
	var b strings.Builder
	for _, l := range r.log.snapshot() {
		b.WriteString(l.text)
		b.WriteString("\n")
	}
	return b.String()
}

// resumeFixture is a loopback swarm whose seeder is slow enough to interrupt a
// download after the first piece verifies. The content is eight 64 KiB pieces,
// so plenty of work remains after the interrupt.
type resumeFixture struct {
	clientBin   string
	torrentPath string
	meta        *metainfo.MetaInfo
	payload     []byte
	pieceLength int64
	outPath     string
	store       *state.Store
	seeder      *recordedProcess
	devtracker  *recordedProcess
}

func startResumeFixture(t *testing.T) *resumeFixture {
	t.Helper()
	root := moduleRoot(t)
	binDir := t.TempDir()
	devtrackerBin := buildBinary(t, root, binDir, "./cmd/devtracker")
	seedBin := buildBinary(t, root, binDir, "./cmd/seed")
	clientBin := buildBinary(t, root, binDir, "./cmd/torrent-client")

	devtracker := startRecorded(t, devtrackerBin, "-addr", "127.0.0.1:0", "-interval", "1")
	line := devtracker.waitFor(t, "listening on", 20*time.Second)
	addr := strings.TrimSpace(line[strings.Index(line, "listening on ")+len("listening on "):])
	announceURL := "http://" + addr + "/announce"

	const pieceLength = 64 * 1024
	payload := make([]byte, 8*pieceLength)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	workDir := t.TempDir()
	srcPath := filepath.Join(workDir, "payload.bin")
	if err := os.WriteFile(srcPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	torrentPath := filepath.Join(workDir, "payload.torrent")
	writeTorrent(t, torrentPath, announceURL, "payload.bin", pieceLength, payload)

	meta, err := metainfo.Load(torrentPath)
	if err != nil {
		t.Fatalf("metainfo.Load: %v", err)
	}

	// A per-block delay widens the transfer so it can be killed after the
	// first piece verifies but well before the last.
	seeder := startRecorded(t, seedBin, "-serve-delay", "25ms", torrentPath, srcPath)
	seeder.waitFor(t, "seed: ready", 20*time.Second)

	outPath := filepath.Join(t.TempDir(), "resumed.bin")
	store := state.Open(outPath, meta.InfoHash, meta.Info.PieceLength, meta.TotalLength(), meta.PieceCount())
	return &resumeFixture{
		clientBin:   clientBin,
		torrentPath: torrentPath,
		meta:        meta,
		payload:     payload,
		pieceLength: pieceLength,
		outPath:     outPath,
		store:       store,
		seeder:      seeder,
		devtracker:  devtracker,
	}
}

// waitForResume polls until the sidecar records at least one verified piece and
// returns that set. The sidecar is written after each verified piece, so its
// appearance also proves an unclean kill would keep that piece.
func waitForResume(t *testing.T, store *state.Store, timeout time.Duration) map[int]bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := store.Load()
		if err == nil {
			held := map[int]bool{}
			for i := 0; i < st.Pieces; i++ {
				if wire.BitfieldHas(st.Have, i) {
					held[i] = true
				}
			}
			if len(held) > 0 {
				return held
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no resume sidecar with verified pieces appeared within %s", timeout)
	return nil
}

// killAfterFirstPiece waits for the first verified piece and the sidecar that
// records it, then SIGKILLs the client so it gets no chance to flush anything.
func (f *resumeFixture) killAfterFirstPiece(t *testing.T, c *clientProc) map[int]bool {
	t.Helper()
	c.waitFor(t, "verified (", 60*time.Second)
	held := waitForResume(t, f.store, 30*time.Second)
	if err := c.cmd.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL the client: %v", err)
	}
	_ = c.wait()
	return held
}

// runToCompletion runs a second client against the same torrent and output and
// returns its stderr.
func (f *resumeFixture) runToCompletion(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, f.clientBin, f.torrentPath, f.outPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("resumed client failed: %v\nstderr:\n%s", err, stderr.String())
	}
	return stderr.String()
}

// servedCounts maps each (piece, begin) the seeder answered to how many times
// it answered it, across every run in the test.
func (f *resumeFixture) servedCounts(t *testing.T) map[[2]uint32]int {
	t.Helper()
	counts := make(map[[2]uint32]int)
	for _, b := range f.seeder.parseServed(t) {
		counts[b.key]++
	}
	return counts
}

func totalBlocks(meta *metainfo.MetaInfo) int {
	n := 0
	for i := 0; i < meta.PieceCount(); i++ {
		n += int((meta.PieceSize(i) + wire.BlockSize - 1) / wire.BlockSize)
	}
	return n
}

// TestEndToEndResumeAfterSIGKILL kills a real download mid-transfer, restarts
// it, and proves both halves of resume: the output is byte-identical, and no
// block of a piece the sidecar had already verified was served a second time —
// the seeder's own block log is the evidence, not the client's word.
func TestEndToEndResumeAfterSIGKILL(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end resume skipped in -short mode")
	}
	f := startResumeFixture(t)
	c1 := startClient(t, f.clientBin, f.torrentPath, f.outPath)
	held := f.killAfterFirstPiece(t, c1)
	t.Logf("killed after %d verified piece(s)", len(held))

	stderr2 := f.runToCompletion(t)
	t.Logf("resumed client stderr:\n%s", stderr2)
	if !strings.Contains(stderr2, "resume:") {
		t.Fatalf("the resumed run did not restore state from the sidecar:\n%s", stderr2)
	}
	assertFileEquals(t, f.outPath, f.payload)

	if _, err := f.store.Load(); !errors.Is(err, state.ErrNoState) {
		t.Fatalf("sidecar survived a completed download: %v", err)
	}

	counts := f.servedCounts(t)
	want := totalBlocks(f.meta)
	if len(counts) != want {
		t.Fatalf("the swarm served %d distinct blocks, want %d", len(counts), want)
	}
	checked := 0
	for key, n := range counts {
		if !held[int(key[0])] {
			continue
		}
		checked++
		if n != 1 {
			t.Fatalf("block piece=%d begin=%d of an already-verified piece was served %d times, want 1", key[0], key[1], n)
		}
	}
	if checked == 0 {
		t.Fatal("no block of a held piece was served, so the resume proof is vacuous")
	}
	t.Logf("%d block(s) of already-verified pieces were served exactly once", checked)

	if served := len(f.seeder.parseServed(t)); served >= 2*want {
		t.Fatalf("the seeder served %d blocks, want fewer than %d: the whole download ran twice", served, 2*want)
	}
}

// TestEndToEndResumeRefetchesTamperedPiece corrupts a verified piece on disk
// after the kill while the sidecar still claims it. The resumed run must re-hash
// what is on disk, reject the claim, re-fetch the piece, and still end
// byte-identical: verify-then-trust, never blind trust.
func TestEndToEndResumeRefetchesTamperedPiece(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end resume skipped in -short mode")
	}
	f := startResumeFixture(t)
	c1 := startClient(t, f.clientBin, f.torrentPath, f.outPath)
	held := f.killAfterFirstPiece(t, c1)

	tampered := -1
	for i := range held {
		if tampered == -1 || i < tampered {
			tampered = i
		}
	}
	off := int64(tampered) * f.pieceLength
	fh, err := os.OpenFile(f.outPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	if _, err := fh.WriteAt([]byte{^f.payload[off]}, off); err != nil {
		t.Fatalf("tamper output: %v", err)
	}
	if err := fh.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("corrupted piece %d at offset %d", tampered, off)

	stderr2 := f.runToCompletion(t)
	assertFileEquals(t, f.outPath, f.payload)
	if !strings.Contains(stderr2, "dropped") {
		t.Fatalf("the resumed run did not report dropping the tampered piece:\n%s", stderr2)
	}

	counts := f.servedCounts(t)
	refetched := 0
	for key, n := range counts {
		if int(key[0]) != tampered {
			continue
		}
		refetched++
		if n < 2 {
			t.Fatalf("tampered piece block begin=%d was served %d times, want at least 2 (the corrupt copy was trusted)", key[1], n)
		}
	}
	if refetched == 0 {
		t.Fatalf("no block of the tampered piece %d was ever served", tampered)
	}
}

// TestEndToEndCleanShutdownFlushesAndAnnouncesStopped sends SIGINT to a live
// download: the client must flush resume state, exit cleanly, and announce
// stopped to the tracker within its bounded deadline.
func TestEndToEndCleanShutdownFlushesAndAnnouncesStopped(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end resume skipped in -short mode")
	}
	f := startResumeFixture(t)
	c := startClient(t, f.clientBin, f.torrentPath, f.outPath)
	c.waitFor(t, "verified (", 60*time.Second)
	held := waitForResume(t, f.store, 30*time.Second)

	if err := c.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("SIGINT: %v", err)
	}
	if err := c.waitExit(t, 10*time.Second); err != nil {
		t.Fatalf("interrupted client exited with %v, want 0\nlog:\n%s", err, c.logText())
	}
	if c.countLines("interrupted; resume state saved") == 0 {
		t.Fatalf("the client did not report a clean interrupt:\n%s", c.logText())
	}

	st, err := f.store.Load()
	if err != nil {
		t.Fatalf("the resume state was not flushed on shutdown: %v", err)
	}
	for i := range held {
		if !wire.BitfieldHas(st.Have, i) {
			t.Fatalf("the flushed sidecar lost already-verified piece %d", i)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(f.devtracker.logText(), `event="stopped"`) {
			t.Log("dev tracker saw the stopped announce")
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the dev tracker never saw a stopped announce:\n%s", f.devtracker.logText())
}
