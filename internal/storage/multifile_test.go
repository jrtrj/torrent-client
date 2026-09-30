package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// multiFileSpec is a small tree whose file boundaries deliberately land inside
// pieces and blocks. That straddling case is the whole difficulty of
// multi-file support: a piece can cover the tail of one file and the head of
// the next.
func multiFileSpec() ([]FileSpec, int64) {
	return []FileSpec{
		{Path: []string{"a.bin"}, Length: 5},
		{Path: []string{"sub", "b.bin"}, Length: 11},
		{Path: []string{"c.bin"}, Length: 9},
	}, 25
}

// writeStream fills the store with the byte pattern 0,1,2,... so every file can
// be checked against its exact slice of the stream.
func writeStream(t *testing.T, s *Storage, total int64, pieceLength int) []byte {
	t.Helper()
	stream := make([]byte, total)
	for i := range stream {
		stream[i] = byte(i)
	}
	for i := 0; int64(i)*int64(pieceLength) < total; i++ {
		start := i * pieceLength
		end := start + pieceLength
		if end > len(stream) {
			end = len(stream)
		}
		if err := s.WritePiece(i, stream[start:end]); err != nil {
			t.Fatalf("WritePiece(%d): %v", i, err)
		}
	}
	return stream
}

func TestMultiFileWritesEachFileAtItsSliceOfTheStream(t *testing.T) {
	dir := t.TempDir()
	files, total := multiFileSpec()
	const pieceLength = 8

	s, err := OpenMulti(dir, files, pieceLength, total)
	if err != nil {
		t.Fatalf("OpenMulti: %v", err)
	}
	if got := s.Files(); got != 3 {
		t.Fatalf("Files() = %d, want 3", got)
	}
	stream := writeStream(t, s, total, pieceLength)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Every file must hold exactly its slice, including the one in the
	// subdirectory, and nothing may be left over from another file.
	want := []struct {
		path string
		data []byte
	}{
		{filepath.Join(dir, "a.bin"), stream[0:5]},
		{filepath.Join(dir, "sub", "b.bin"), stream[5:16]},
		{filepath.Join(dir, "c.bin"), stream[16:25]},
	}
	for _, w := range want {
		got, err := os.ReadFile(w.path)
		if err != nil {
			t.Fatalf("read %s: %v", w.path, err)
		}
		if !bytes.Equal(got, w.data) {
			t.Errorf("%s = %x, want %x", w.path, got, w.data)
		}
	}
}

func TestMultiFileReadBlockStraddlesFileBoundaries(t *testing.T) {
	dir := t.TempDir()
	files, total := multiFileSpec()
	const pieceLength = 8

	s, err := OpenMulti(dir, files, pieceLength, total)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	stream := writeStream(t, s, total, pieceLength)

	// Piece 0 covers a.bin entirely (5 bytes) plus the head of sub/b.bin.
	got, err := s.ReadBlock(0, 0, 8)
	if err != nil {
		t.Fatalf("ReadBlock across a boundary: %v", err)
	}
	if !bytes.Equal(got, stream[0:8]) {
		t.Errorf("straddling block = %x, want %x", got, stream[0:8])
	}

	// The tail of the short final piece, which ends inside c.bin.
	got, err = s.ReadBlock(3, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, stream[24:25]) {
		t.Errorf("tail block = %x, want %x", got, stream[24:25])
	}
}

// A hostile .torrent can name a file anything, including a traversal. Nothing
// may be created, inside or outside the output directory, when one is refused.
func TestMultiFileRefusesPathsThatEscapeTheOutputDirectory(t *testing.T) {
	cases := []struct {
		name string
		path []string
	}{
		{"parent", []string{"..", "evil"}},
		{"parent in the middle", []string{"sub", "..", "evil"}},
		{"absolute", []string{"/etc", "passwd"}},
		{"windows drive", []string{`C:\evil`}},
		{"separator inside a segment", []string{"a/b"}},
		{"backslash", []string{`a\b`}},
		{"current directory", []string{"."}},
		{"empty segment", []string{""}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if _, err := OpenMulti(dir, []FileSpec{{Path: tt.path, Length: 4}}, 4, 4); err == nil {
				t.Fatalf("OpenMulti accepted the path %q", tt.path)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("a refused torrent created %d entries in the output directory", len(entries))
			}
		})
	}
}

func TestMultiFileRejectsAFileListThatContradictsTheLength(t *testing.T) {
	if _, err := OpenMulti(t.TempDir(), []FileSpec{{Path: []string{"a"}, Length: 4}}, 4, 99); err == nil {
		t.Fatal("accepted a file list that does not sum to the content length")
	}
}

// Thousands of files would exhaust the process's descriptors, so the handle
// cache evicts. Eviction must not corrupt anything.
func TestMultiFileSurvivesMoreFilesThanTheHandleCache(t *testing.T) {
	dir := t.TempDir()
	const n = maxOpenFiles * 3
	files := make([]FileSpec, n)
	var total int64
	for i := range files {
		files[i] = FileSpec{Path: []string{fmt.Sprintf("f%03d.bin", i)}, Length: 4}
		total += 4
	}

	s, err := OpenMulti(dir, files, 4, total)
	if err != nil {
		t.Fatal(err)
	}
	for i := range files {
		if err := s.WritePiece(i, []byte{byte(i), 1, 2, 3}); err != nil {
			t.Fatalf("WritePiece(%d): %v", i, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	for i := range files {
		got, err := os.ReadFile(filepath.Join(dir, files[i].Path[0]))
		if err != nil {
			t.Fatalf("read file %d: %v", i, err)
		}
		if !bytes.Equal(got, []byte{byte(i), 1, 2, 3}) {
			t.Fatalf("file %d = %x, want %x", i, got, []byte{byte(i), 1, 2, 3})
		}
	}
}

// A single-file torrent is the degenerate case of the same mapping: one file
// covering the whole stream. Keeping it that way is what stops the download,
// resume and seed paths needing two code paths.
func TestSingleFileIsOneSpanOfTheSameMapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "one.bin")
	s, err := Open(path, 8, 25)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Files(); got != 1 {
		t.Fatalf("Files() = %d, want 1 for a single-file torrent", got)
	}
	stream := writeStream(t, s, 25, 8)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, stream) {
		t.Errorf("file = %x, want %x", got, stream)
	}
}
