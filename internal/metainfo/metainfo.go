// Package metainfo parses .torrent files (BEP 3) into the fields the download
// and seed paths need.
//
// The info-hash is SHA-1 over the raw `info` dictionary bytes exactly as they
// appear in the file. Re-encoding the parsed values instead can reorder keys
// and silently produce a hash that no tracker or peer will recognise, so the
// raw bytes are captured at decode time with bencode.RawMessage and never
// rebuilt.
package metainfo

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"torrent-client/internal/bencode"
)

// HashSize is the length of a piece hash and of the info-hash.
const HashSize = 20

// File is one entry of a multi-file torrent's `files` list.
type File struct {
	Length int64
	Path   []string
}

// Info is the parsed `info` dictionary. Exactly one of Length (single-file) or
// Files (multi-file) describes the content.
type Info struct {
	Name        string
	PieceLength int64
	PieceHashes []byte
	Length      int64
	Files       []File
}

// MetaInfo is a parsed .torrent file.
type MetaInfo struct {
	Announce     string
	AnnounceList [][]string
	Info         Info
	InfoHash     [HashSize]byte
}

// Multifile reports whether the torrent describes a set of files.
func (m *MetaInfo) Multifile() bool { return len(m.Info.Files) > 0 }

// TotalLength is the length of the contiguous piece stream, i.e. the summed
// length of every file.
func (m *MetaInfo) TotalLength() int64 {
	if !m.Multifile() {
		return m.Info.Length
	}
	var total int64
	for _, f := range m.Info.Files {
		total += f.Length
	}
	return total
}

// PieceCount is derived from the hash string, not from the length: a truncated
// or padded `pieces` value is the corruption worth catching early.
func (m *MetaInfo) PieceCount() int {
	if m.Info.PieceLength <= 0 {
		return 0
	}
	return len(m.Info.PieceHashes) / HashSize
}

// PieceSize returns the byte length of piece i. Only the last piece may be
// shorter than the piece length.
func (m *MetaInfo) PieceSize(i int) int64 {
	if i < 0 || i >= m.PieceCount() {
		return 0
	}
	if i == m.PieceCount()-1 {
		if rem := m.TotalLength() % m.Info.PieceLength; rem != 0 {
			return rem
		}
	}
	return m.Info.PieceLength
}

// PieceHash returns the 20-byte expected hash of piece i.
func (m *MetaInfo) PieceHash(i int) []byte {
	start := i * HashSize
	return m.Info.PieceHashes[start : start+HashSize]
}

// Trackers returns the announce URL followed by every URL in announce-list,
// de-duplicated and in discovery order.
func (m *MetaInfo) Trackers() []string {
	var out []string
	seen := make(map[string]bool)
	add := func(u string) {
		if u == "" || seen[u] {
			return
		}
		seen[u] = true
		out = append(out, u)
	}
	add(m.Announce)
	for _, tier := range m.AnnounceList {
		for _, u := range tier {
			add(u)
		}
	}
	return out
}

// Load reads and parses a .torrent file.
func Load(path string) (*MetaInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	m, err := Parse(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return m, nil
}

// Parse decodes one .torrent stream.
func Parse(r io.Reader) (*MetaInfo, error) {
	var raw struct {
		Announce     string             `bencode:"announce"`
		AnnounceList [][]string         `bencode:"announce-list"`
		Info         bencode.RawMessage `bencode:"info"`
	}
	if err := bencode.Unmarshal(r, &raw); err != nil {
		return nil, fmt.Errorf("parse metainfo: %w", err)
	}
	if len(raw.Info) == 0 {
		return nil, errors.New("metainfo: missing info dictionary")
	}

	// `pieces` is binary, and the codec does not assign bencode strings into
	// []byte fields, so it is read as a string and converted.
	var info struct {
		Name        string `bencode:"name"`
		PieceLength int64  `bencode:"piece length"`
		Pieces      string `bencode:"pieces"`
		Length      int64  `bencode:"length"`
		Files       []struct {
			Length int64    `bencode:"length"`
			Path   []string `bencode:"path"`
		} `bencode:"files"`
	}
	if err := bencode.Unmarshal(bytes.NewReader(raw.Info), &info); err != nil {
		return nil, fmt.Errorf("parse info dictionary: %w", err)
	}

	m := &MetaInfo{Announce: raw.Announce, AnnounceList: raw.AnnounceList}
	m.InfoHash = sha1.Sum(raw.Info)
	m.Info = Info{
		Name:        info.Name,
		PieceLength: info.PieceLength,
		PieceHashes: []byte(info.Pieces),
		Length:      info.Length,
	}
	for _, f := range info.Files {
		m.Info.Files = append(m.Info.Files, File{Length: f.Length, Path: f.Path})
	}

	if err := m.validate(); err != nil {
		return nil, err
	}
	return m, nil
}

// validPathSegments rejects a path that could escape the output directory. The
// torrent format puts no restriction on these strings, so a hostile .torrent
// can legitimately declare "..", an absolute path or a Windows drive; the
// parser refuses one here rather than letting it reach the filesystem. This is
// belt to storage's braces: both layers check, so a caller that bypasses one
// is still covered by the other.
func validPathSegments(segs []string) error {
	for _, s := range segs {
		switch {
		case s == "":
			return errors.New("path segment is empty")
		case s == "." || s == "..":
			return fmt.Errorf("path segment %q is a relative directory reference", s)
		case strings.ContainsRune(s, 0):
			return errors.New("path segment contains a NUL byte")
		case strings.ContainsAny(s, `/\`):
			return fmt.Errorf("path segment %q contains a path separator", s)
		case strings.ContainsRune(s, ':'):
			return fmt.Errorf("path segment %q contains a colon", s)
		}
	}
	return nil
}

func (m *MetaInfo) validate() error {
	if m.Info.Name == "" {
		return errors.New("metainfo: info dictionary has no name")
	}
	// The name becomes a file (single-file) or a directory (multi-file) under
	// the output path, so it must be one safe path segment.
	if err := validPathSegments([]string{m.Info.Name}); err != nil {
		return fmt.Errorf("metainfo: name %q: %w", m.Info.Name, err)
	}
	if m.Info.PieceLength <= 0 {
		return fmt.Errorf("metainfo: piece length %d is not positive", m.Info.PieceLength)
	}
	if len(m.Info.PieceHashes) == 0 || len(m.Info.PieceHashes)%HashSize != 0 {
		return fmt.Errorf("metainfo: pieces is %d bytes, want a positive multiple of %d", len(m.Info.PieceHashes), HashSize)
	}
	if m.Multifile() {
		for i, f := range m.Info.Files {
			if f.Length < 0 {
				return fmt.Errorf("metainfo: file %d has negative length", i)
			}
			if len(f.Path) == 0 {
				return fmt.Errorf("metainfo: file %d has an empty path", i)
			}
			if err := validPathSegments(f.Path); err != nil {
				return fmt.Errorf("metainfo: file %d: %w", i, err)
			}
		}
	} else if m.Info.Length <= 0 {
		return fmt.Errorf("metainfo: single-file length %d is not positive", m.Info.Length)
	}

	total := m.TotalLength()
	if total <= 0 {
		return errors.New("metainfo: content length is zero")
	}
	if want := (total + m.Info.PieceLength - 1) / m.Info.PieceLength; int64(m.PieceCount()) != want {
		return fmt.Errorf("metainfo: %d piece hashes contradict the %d pieces implied by length %d and piece length %d",
			m.PieceCount(), want, total, m.Info.PieceLength)
	}
	return nil
}
