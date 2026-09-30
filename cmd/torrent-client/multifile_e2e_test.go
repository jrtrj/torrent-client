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
)

type multiFileEntry struct {
	Length int64    `bencode:"length"`
	Path   []string `bencode:"path"`
}

type multiFileInfo struct {
	Name        string           `bencode:"name"`
	PieceLength int64            `bencode:"piece length"`
	Pieces      []byte           `bencode:"pieces"`
	Files       []multiFileEntry `bencode:"files"`
}

type fixtureFile struct {
	path []string
	data []byte
}

func fixtureBlob(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return strings.ToLower(strings.TrimSpace(sprintfHex(sum)))
}

func sprintfHex(b [32]byte) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 0, 64)
	for _, c := range b {
		out = append(out, hex[c>>4], hex[c&0x0f])
	}
	return string(out)
}

func devtrackerAddr(t *testing.T, p *process) string {
	t.Helper()
	line := p.waitForLine(t, "listening on", 20*time.Second)
	const marker = "listening on "
	i := strings.Index(line, marker)
	if i < 0 {
		t.Fatalf("devtracker said %q, which has no address", line)
	}
	return strings.TrimSpace(line[i+len(marker):])
}

// Pieces span the concatenated bytes of every file — that's what makes file
// boundaries land mid-piece.
func writeMultiFileTorrent(t *testing.T, path, announce, name string, pieceLength int64, files []fixtureFile) {
	t.Helper()

	var stream []byte
	entries := make([]multiFileEntry, 0, len(files))
	for _, f := range files {
		stream = append(stream, f.data...)
		entries = append(entries, multiFileEntry{Length: int64(len(f.data)), Path: f.path})
	}

	var pieces []byte
	for off := 0; off < len(stream); off += int(pieceLength) {
		end := off + int(pieceLength)
		if end > len(stream) {
			end = len(stream)
		}
		sum := sha1.Sum(stream[off:end])
		pieces = append(pieces, sum[:]...)
	}

	infoBytes := marshalBencode(t, multiFileInfo{
		Name:        name,
		PieceLength: pieceLength,
		Pieces:      pieces,
		Files:       entries,
	})
	torrent := marshalBencode(t, struct {
		Announce string             `bencode:"announce"`
		Info     bencode.RawMessage `bencode:"info"`
	}{Announce: announce, Info: infoBytes})

	if err := os.WriteFile(path, torrent, 0o644); err != nil {
		t.Fatal(err)
	}
}

// materialise writes the fixture tree under root so the seeder has something to
// serve and we have something to compare the download against.
func materialise(t *testing.T, root string, files []fixtureFile) {
	t.Helper()
	for _, f := range files {
		p := filepath.Join(append([]string{root}, f.path...)...)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, f.data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// The multi-file proof: a real directory tree, seeded by cmd/seed and fetched by
// the real CLI over the wire, every file compared byte-for-byte afterwards.
//
// Sizes are picked so the awkward cases are all in play: the first file isn't a
// whole number of pieces, a boundary falls mid-piece, files sit one and two
// directories deep, and the last piece is short.
func TestEndToEndMultiFileDownload(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end multi-file download skipped in -short mode")
	}

	root := moduleRoot(t)
	binDir := t.TempDir()
	devtrackerBin := buildBinary(t, root, binDir, "./cmd/devtracker")
	seedBin := buildBinary(t, root, binDir, "./cmd/seed")
	clientBin := buildBinary(t, root, binDir, "./cmd/torrent-client")

	devtracker := startProcess(t, devtrackerBin, "-addr", "127.0.0.1:0", "-interval", "2")
	announceURL := "http://" + devtrackerAddr(t, devtracker) + "/announce"

	const pieceLength = 32 * 1024
	files := []fixtureFile{
		{path: []string{"first.bin"}, data: fixtureBlob(t, 100*1024)},
		{path: []string{"nested", "second.bin"}, data: fixtureBlob(t, 40*1024)},
		{path: []string{"nested", "deep", "third.bin"}, data: fixtureBlob(t, 1)},
	}

	work := t.TempDir()
	torrentPath := filepath.Join(work, "bundle.torrent")
	writeMultiFileTorrent(t, torrentPath, announceURL, "bundle", pieceLength, files)

	// The seeder gets the parent directory: the torrent's name becomes the root
	// inside it, the same layout the client writes.
	srcParent := filepath.Join(work, "src")
	materialise(t, filepath.Join(srcParent, "bundle"), files)

	seeder := startProcess(t, seedBin, torrentPath, srcParent)
	seeder.waitForLine(t, "seed: ready", 20*time.Second)

	outDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := exec.CommandContext(ctx, clientBin, torrentPath, outDir)
	var stdout, stderr bytes.Buffer
	client.Stdout = &stdout
	client.Stderr = &stderr
	if err := client.Run(); err != nil {
		t.Fatalf("client failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	for _, f := range files {
		gotPath := filepath.Join(append([]string{outDir, "bundle"}, f.path...)...)
		got, err := os.ReadFile(gotPath)
		if err != nil {
			t.Fatalf("read %s: %v\nstderr:\n%s", gotPath, err, stderr.String())
		}
		if !bytes.Equal(got, f.data) {
			t.Errorf("%s: %d bytes sha256 %s; want %d bytes sha256 %s",
				gotPath, len(got), digest(got), len(f.data), digest(f.data))
		}
	}
}

// startProcess with stderr folded into the same pipe: the fixture binaries
// announce on stdout, the client logs progress on stderr — the dashboard owns
// that stream — so a test waiting on a client's log line has to read both.
func startMergedProcess(t *testing.T, name string, args ...string) *process {
	t.Helper()
	cmd := exec.Command(name, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
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

// The seed read path has to walk the same mapping the write path built: download
// the tree, keep seeding, then serve it to a second client with no other source.
func TestEndToEndMultiFileSeedsBackToAnotherClient(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end multi-file seeding skipped in -short mode")
	}

	root := moduleRoot(t)
	binDir := t.TempDir()
	devtrackerBin := buildBinary(t, root, binDir, "./cmd/devtracker")
	seedBin := buildBinary(t, root, binDir, "./cmd/seed")
	clientBin := buildBinary(t, root, binDir, "./cmd/torrent-client")

	devtracker := startProcess(t, devtrackerBin, "-addr", "127.0.0.1:0", "-interval", "2")
	announceURL := "http://" + devtrackerAddr(t, devtracker) + "/announce"

	const pieceLength = 32 * 1024
	files := []fixtureFile{
		{path: []string{"a.bin"}, data: fixtureBlob(t, 70*1024)},
		{path: []string{"b", "c.bin"}, data: fixtureBlob(t, 50*1024)},
	}

	work := t.TempDir()
	torrentPath := filepath.Join(work, "bundle.torrent")
	writeMultiFileTorrent(t, torrentPath, announceURL, "bundle", pieceLength, files)
	srcParent := filepath.Join(work, "src")
	materialise(t, filepath.Join(srcParent, "bundle"), files)

	fixtureSeeder := startProcess(t, seedBin, torrentPath, srcParent)
	fixtureSeeder.waitForLine(t, "seed: ready", 20*time.Second)

	// A downloads the tree, then keeps seeding it.
	a := startMergedProcess(t, clientBin, "-seed", "-port", "7003", torrentPath, filepath.Join(t.TempDir(), "a"))
	a.waitForLine(t, "engine: complete:", 90*time.Second)

	// Stop the only other source, so B can only be served by A.
	if err := fixtureSeeder.cmd.Process.Kill(); err != nil {
		t.Fatalf("stop the fixture seeder: %v", err)
	}
	_ = fixtureSeeder.cmd.Wait()

	outDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := exec.CommandContext(ctx, clientBin, torrentPath, outDir)
	var stderr bytes.Buffer
	client.Stderr = &stderr
	if err := client.Run(); err != nil {
		t.Fatalf("second client failed: %v\nstderr:\n%s", err, stderr.String())
	}

	for _, f := range files {
		got, err := os.ReadFile(filepath.Join(append([]string{outDir, "bundle"}, f.path...)...))
		if err != nil {
			t.Fatalf("read %v: %v", f.path, err)
		}
		if !bytes.Equal(got, f.data) {
			t.Errorf("%v differs after being served by our own client", f.path)
		}
	}
}
