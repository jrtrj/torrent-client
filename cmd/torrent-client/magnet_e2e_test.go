package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The magnet proof: the client gets an info-hash and a tracker, nothing else.
// Find a peer, pull the info dictionary over ut_metadata, check it against the
// hash, then download. No .torrent file exists anywhere near the path it's
// given, so the metadata can only have come off the wire.
func TestEndToEndMagnetDownload(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end magnet download skipped in -short mode")
	}

	root := moduleRoot(t)
	binDir := t.TempDir()
	devtrackerBin := buildBinary(t, root, binDir, "./cmd/devtracker")
	seedBin := buildBinary(t, root, binDir, "./cmd/seed")
	clientBin := buildBinary(t, root, binDir, "./cmd/torrent-client")

	devtracker := startProcess(t, devtrackerBin, "-addr", "127.0.0.1:0", "-interval", "2")
	announceURL := "http://" + devtrackerAddr(t, devtracker) + "/announce"

	const pieceLength = 32 * 1024
	payload := fixtureBlob(t, 150*1024)

	work := t.TempDir()
	torrentPath := filepath.Join(work, "single.torrent")
	infoBytes := writeTorrent(t, torrentPath, announceURL, "single.bin", pieceLength, payload)
	infoHash := sha1.Sum(infoBytes)

	// The swarm's only member, and our only source of metadata and content.
	srcDir := filepath.Join(work, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "single.bin"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	seeder := startProcess(t, seedBin, torrentPath, srcDir)
	seeder.waitForLine(t, "seed: ready", 20*time.Second)

	magnetURI := "magnet:?xt=urn:btih:" + hex.EncodeToString(infoHash[:]) +
		"&dn=single.bin&tr=" + url.QueryEscape(announceURL)

	outDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := exec.CommandContext(ctx, clientBin, magnetURI, outDir)
	var stdout, stderr bytes.Buffer
	client.Stdout = &stdout
	client.Stderr = &stderr
	if err := client.Run(); err != nil {
		t.Fatalf("the magnet download failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	got, err := os.ReadFile(filepath.Join(outDir, "single.bin"))
	if err != nil {
		t.Fatalf("read the downloaded file: %v\nstderr:\n%s", err, stderr.String())
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("downloaded %d bytes, want the %d seeded bytes", len(got), len(payload))
	}
	// And the log has to show the metadata really came over the wire.
	if !strings.Contains(stderr.String(), "resolving magnet") {
		t.Errorf("the client never reported resolving the magnet:\n%s", stderr.String())
	}
}

// The other half of the proof: the info-hash is all the client knows, so a hash
// matching nothing in the swarm has to fail loudly instead of downloading
// whatever it stumbles on. Exit 0 here and the user believes they got their file.
func TestEndToEndMagnetDownloadFailsOnAnUnknownHash(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end magnet download skipped in -short mode")
	}

	root := moduleRoot(t)
	binDir := t.TempDir()
	devtrackerBin := buildBinary(t, root, binDir, "./cmd/devtracker")
	seedBin := buildBinary(t, root, binDir, "./cmd/seed")
	clientBin := buildBinary(t, root, binDir, "./cmd/torrent-client")

	devtracker := startProcess(t, devtrackerBin, "-addr", "127.0.0.1:0", "-interval", "2")
	announceURL := "http://" + devtrackerAddr(t, devtracker) + "/announce"

	// A real torrent is seeded, but the magnet asks for a different one.
	payload := fixtureBlob(t, 32*1024)
	work := t.TempDir()
	torrentPath := filepath.Join(work, "single.torrent")
	writeTorrent(t, torrentPath, announceURL, "single.bin", 32*1024, payload)

	srcDir := filepath.Join(work, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "single.bin"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	seeder := startProcess(t, seedBin, torrentPath, srcDir)
	seeder.waitForLine(t, "seed: ready", 20*time.Second)

	// Nothing in the swarm has this hash. It's well-formed, so only the hash check
	// itself can catch it.
	var bogus [20]byte
	for i := range bogus {
		bogus[i] = byte(i)
	}

	magnetURI := "magnet:?xt=urn:btih:" + hex.EncodeToString(bogus[:]) +
		"&tr=" + url.QueryEscape(announceURL)

	outDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := exec.CommandContext(ctx, clientBin, magnetURI, outDir)
	var stderr bytes.Buffer
	client.Stderr = &stderr

	err := client.Run()
	if err == nil {
		t.Fatalf("the client exited 0 for an info-hash nothing in the swarm has\nstderr:\n%s", stderr.String())
	}
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 1 {
		t.Fatalf("exit = %v, want exit status 1 (fatal)\nstderr:\n%s", err, stderr.String())
	}
	if _, statErr := os.Stat(filepath.Join(outDir, "single.bin")); statErr == nil {
		t.Fatal("a file was written for a magnet the client could not resolve")
	}
}
