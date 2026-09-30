package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWritePieceAndReadBlock(t *testing.T) {
	// The "nested" directory doesn't exist yet, and Open has to create it.
	path := filepath.Join(t.TempDir(), "nested", "data.bin")
	s, err := Open(path, 4, 10)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	for i, piece := range [][]byte{[]byte("abcd"), []byte("efgh"), []byte("ij")} {
		if err := s.WritePiece(i, piece); err != nil {
			t.Fatalf("WritePiece(%d): %v", i, err)
		}
	}

	got, err := s.ReadBlock(2, 0, 2)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if !bytes.Equal(got, []byte("ij")) {
		t.Fatalf("ReadBlock = %q, want %q", got, "ij")
	}

	onDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(onDisk, []byte("abcdefghij")) {
		t.Fatalf("file = %q, want %q", onDisk, "abcdefghij")
	}
	if s.Length() != 10 {
		t.Fatalf("Length = %d, want 10", s.Length())
	}
}

func TestReadBlockSpansTheBlockOffset(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "d.bin"), 4, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.WritePiece(1, []byte("WXYZ")); err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadBlock(1, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("XY")) {
		t.Fatalf("ReadBlock(1,1,2) = %q, want %q", got, "XY")
	}
}

func TestOutOfRangeAccessIsRejected(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "d.bin"), 4, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.WritePiece(0, []byte("12345")); err == nil {
		t.Fatal("a piece longer than the piece length was accepted")
	}
	if err := s.WritePiece(1, []byte("12345")); err == nil {
		t.Fatal("a piece overrunning the content was accepted")
	}
	if err := s.WritePiece(-1, []byte("x")); err == nil {
		t.Fatal("a negative piece index was accepted")
	}
	if _, err := s.ReadBlock(1, 4, 4); err == nil {
		t.Fatal("a read past the end was accepted")
	}
}

func TestOpenRejectsBadGeometry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "d.bin")
	if _, err := Open(path, 0, 10); err == nil {
		t.Fatal("piece length 0 was accepted")
	}
	if _, err := Open(path, 4, -1); err == nil {
		t.Fatal("negative content length was accepted")
	}
}
