package metainfo

import (
	"bytes"
	"strings"
	"testing"
)

// multiFileInfoDict builds a multi-file info dictionary with one file at path.
func multiFileInfoDict(name string, path []string, length int64) any {
	return struct {
		Name        string     `bencode:"name"`
		PieceLength int64      `bencode:"piece length"`
		Pieces      []byte     `bencode:"pieces"`
		Files       []fileDict `bencode:"files"`
	}{
		Name:        name,
		PieceLength: 16384,
		Pieces:      bytes.Repeat([]byte{0x11}, HashSize),
		Files:       []fileDict{{Length: length, Path: path}},
	}
}

// A .torrent can name a file anything the bencode allows, including a
// traversal, an absolute path or a Windows drive. The parser is the first line
// of defence: it refuses one rather than passing it to the filesystem.
func TestParseRefusesTraversingFilePaths(t *testing.T) {
	bad := [][]string{
		{"..", "evil"},
		{"sub", "..", "evil"},
		{"/etc", "passwd"},
		{`C:\evil`},
		{"a/b"},
		{`a\b`},
		{"."},
		{""},
	}
	for _, path := range bad {
		t.Run(strings.Join(path, "|"), func(t *testing.T) {
			info := multiFileInfoDict("bundle", path, 4)
			torrent := torrentBytes(t, "http://t/announce", nil, marshal(t, info))
			if _, err := Parse(bytes.NewReader(torrent)); err == nil {
				t.Fatalf("Parse accepted the file path %q", path)
			}
		})
	}
}

// The torrent's own name becomes a file (single-file) or a directory
// (multi-file) under the output path, so it has to be one safe segment too.
func TestParseRefusesATraversingName(t *testing.T) {
	for _, name := range []string{"..", ".", "a/b", "/abs", `C:\x`} {
		t.Run(name, func(t *testing.T) {
			single := infoDict{
				Length:      4,
				Name:        name,
				PieceLength: 16384,
				Pieces:      bytes.Repeat([]byte{0x11}, HashSize),
			}
			torrent := torrentBytes(t, "http://t/announce", nil, marshal(t, single))
			if _, err := Parse(bytes.NewReader(torrent)); err == nil {
				t.Fatalf("Parse accepted the single-file name %q", name)
			}

			multi := multiFileInfoDict(name, []string{"ok.bin"}, 4)
			torrent = torrentBytes(t, "http://t/announce", nil, marshal(t, multi))
			if _, err := Parse(bytes.NewReader(torrent)); err == nil {
				t.Fatalf("Parse accepted the multi-file name %q", name)
			}
		})
	}
}

// The defence must not reject ordinary names and nested paths, or real
// torrents would stop working.
func TestParseAcceptsOrdinaryNamesAndNestedPaths(t *testing.T) {
	names := []string{"bundle", "my.torrent.iso", "a-b_c", "Ubuntu 24.04"}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			single := infoDict{
				Length:      4,
				Name:        name,
				PieceLength: 16384,
				Pieces:      bytes.Repeat([]byte{0x11}, HashSize),
			}
			torrent := torrentBytes(t, "http://t/announce", nil, marshal(t, single))
			if _, err := Parse(bytes.NewReader(torrent)); err != nil {
				t.Fatalf("Parse rejected the ordinary name %q: %v", name, err)
			}
		})
	}

	paths := [][]string{
		{"a.bin"},
		{"sub", "b.bin"},
		{"deep", "nested", "c.bin"},
		{"with spaces.bin"},
	}
	for _, path := range paths {
		t.Run(strings.Join(path, "/"), func(t *testing.T) {
			info := multiFileInfoDict("bundle", path, 4)
			torrent := torrentBytes(t, "http://t/announce", nil, marshal(t, info))
			m, err := Parse(bytes.NewReader(torrent))
			if err != nil {
				t.Fatalf("Parse rejected the ordinary path %q: %v", path, err)
			}
			if len(m.Info.Files) != 1 || strings.Join(m.Info.Files[0].Path, "/") != strings.Join(path, "/") {
				t.Fatalf("path came back as %v, want %v", m.Info.Files[0].Path, path)
			}
		})
	}
}
