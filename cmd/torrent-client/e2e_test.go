package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"torrent-client/internal/bencode"
	"torrent-client/internal/metainfo"
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
	lines := make(chan string, 64)
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
