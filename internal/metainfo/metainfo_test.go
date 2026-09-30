package metainfo

import (
	"bytes"
	"crypto/sha1"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"torrent-client/internal/bencode"
)

// infoDict is marshalled by the codec, so the bytes the tests hash come from a
// path independent of the raw capture inside Parse.
type infoDict struct {
	Length      int64  `bencode:"length"`
	Name        string `bencode:"name"`
	PieceLength int64  `bencode:"piece length"`
	Pieces      []byte `bencode:"pieces"`
}

type fileDict struct {
	Length int64    `bencode:"length"`
	Path   []string `bencode:"path"`
}

func marshal(t *testing.T, v any) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := bencode.Marshal(&buf, v); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return buf.Bytes()
}

// torrentBytes wraps raw info bytes in a metainfo file. RawMessage is what
// keeps the info bytes byte-exact rather than re-encoded.
func torrentBytes(t *testing.T, announce string, announceList [][]string, infoBytes []byte) []byte {
	t.Helper()
	return marshal(t, struct {
		Announce     string             `bencode:"announce"`
		AnnounceList [][]string         `bencode:"announce-list"`
		Info         bencode.RawMessage `bencode:"info"`
	}{announce, announceList, infoBytes})
}

func parse(t *testing.T, torrent []byte) *MetaInfo {
	t.Helper()
	m, err := Parse(bytes.NewReader(torrent))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return m
}

// The info-hash is SHA-1 over the literal bytes of the info dictionary, so
// those bytes are written out by hand here: the parser gets nowhere to hide a
// re-encoding mistake.
func TestParseHashesLiteralInfoBytes(t *testing.T) {
	pieceHash := strings.Repeat("\xab", 20)
	info := "d6:lengthi4e4:name8:test.txt12:piece lengthi16384e6:pieces20:" + pieceHash + "e"
	torrent := "d8:announce17:http://t/announce4:info" + info + "e"

	m := parse(t, []byte(torrent))

	if want := sha1.Sum([]byte(info)); m.InfoHash != want {
		t.Fatalf("info-hash = %x, want %x", m.InfoHash, want)
	}
	if m.Info.Name != "test.txt" || m.Info.Length != 4 || m.Info.PieceLength != 16384 {
		t.Fatalf("unexpected info: %+v", m.Info)
	}
	if !bytes.Equal(m.PieceHash(0), []byte(pieceHash)) {
		t.Fatalf("piece hash = %x, want %x", m.PieceHash(0), pieceHash)
	}
	if m.Announce != "http://t/announce" {
		t.Fatalf("announce = %q", m.Announce)
	}
}

func TestParseHashesRawInfoBytesFromMarshalledDict(t *testing.T) {
	info := infoDict{Length: 100, Name: "x.bin", PieceLength: 32, Pieces: bytes.Repeat([]byte{0x9f}, 4*HashSize)}
	infoBytes := marshal(t, info)

	m := parse(t, torrentBytes(t, "http://t/announce", nil, infoBytes))

	if want := sha1.Sum(infoBytes); m.InfoHash != want {
		t.Fatalf("info-hash = %x, want the SHA-1 of the raw info bytes %x", m.InfoHash, want)
	}
}

func TestParseMultiFileSumsLengths(t *testing.T) {
	info := struct {
		Name        string     `bencode:"name"`
		PieceLength int64      `bencode:"piece length"`
		Pieces      []byte     `bencode:"pieces"`
		Files       []fileDict `bencode:"files"`
	}{
		Name:        "bundle",
		PieceLength: 16384,
		Files: []fileDict{
			{Length: 200000, Path: []string{"a.bin"}},
			{Length: 100000, Path: []string{"sub", "b.bin"}},
		},
	}
	const total = 300000
	count := (total + info.PieceLength - 1) / info.PieceLength
	info.Pieces = bytes.Repeat([]byte{0x11}, int(count)*HashSize)

	m := parse(t, torrentBytes(t, "http://t/announce", nil, marshal(t, info)))

	if !m.Multifile() {
		t.Fatal("Multifile = false, want true")
	}
	if m.TotalLength() != total {
		t.Fatalf("TotalLength = %d, want %d", m.TotalLength(), total)
	}
	if m.PieceCount() != int(count) {
		t.Fatalf("PieceCount = %d, want %d", m.PieceCount(), count)
	}
	if want := total - (count-1)*info.PieceLength; m.PieceSize(int(count)-1) != want {
		t.Fatalf("last PieceSize = %d, want %d", m.PieceSize(int(count)-1), want)
	}
	if len(m.Info.Files) != 2 || m.Info.Files[1].Path[1] != "b.bin" {
		t.Fatalf("files = %+v", m.Info.Files)
	}
}

func TestPieceSizeTail(t *testing.T) {
	info := infoDict{Length: 100, Name: "x", PieceLength: 32, Pieces: bytes.Repeat([]byte{1}, 4*HashSize)}
	m := parse(t, torrentBytes(t, "http://t/announce", nil, marshal(t, info)))

	if m.PieceCount() != 4 {
		t.Fatalf("PieceCount = %d, want 4", m.PieceCount())
	}
	for i := 0; i < 3; i++ {
		if got := m.PieceSize(i); got != 32 {
			t.Fatalf("PieceSize(%d) = %d, want 32", i, got)
		}
	}
	if got := m.PieceSize(3); got != 4 {
		t.Fatalf("PieceSize(3) = %d, want 4", got)
	}
	if got := m.PieceSize(99); got != 0 {
		t.Fatalf("PieceSize(out of range) = %d, want 0", got)
	}
}

func TestTrackersOrdersAndDeduplicates(t *testing.T) {
	info := infoDict{Length: 4, Name: "x", PieceLength: 16384, Pieces: bytes.Repeat([]byte{1}, HashSize)}
	announceList := [][]string{
		{"http://b/announce", "http://a/announce"},
		{"http://c/announce"},
	}
	m := parse(t, torrentBytes(t, "http://a/announce", announceList, marshal(t, info)))

	want := []string{"http://a/announce", "http://b/announce", "http://c/announce"}
	got := m.Trackers()
	if len(got) != len(want) {
		t.Fatalf("Trackers = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Trackers = %v, want %v", got, want)
		}
	}
}

func TestParseRejectsMalformedTorrents(t *testing.T) {
	tests := []struct {
		name    string
		torrent []byte
	}{
		{
			name:    "not bencode",
			torrent: []byte("this is not bencode"),
		},
		{
			name: "no info dictionary",
			torrent: marshal(t, struct {
				Announce string `bencode:"announce"`
			}{Announce: "http://t/announce"}),
		},
		{
			name:    "empty name",
			torrent: torrentBytes(t, "http://t/announce", nil, marshal(t, infoDict{Length: 100, Name: "", PieceLength: 16, Pieces: bytes.Repeat([]byte{1}, 140)})),
		},
		{
			name:    "zero piece length",
			torrent: torrentBytes(t, "http://t/announce", nil, marshal(t, infoDict{Length: 100, Name: "x", PieceLength: 0, Pieces: bytes.Repeat([]byte{1}, 140)})),
		},
		{
			name:    "pieces not a multiple of the hash size",
			torrent: torrentBytes(t, "http://t/announce", nil, marshal(t, infoDict{Length: 100, Name: "x", PieceLength: 16, Pieces: bytes.Repeat([]byte{1}, 141)})),
		},
		{
			name:    "piece count contradicts the length",
			torrent: torrentBytes(t, "http://t/announce", nil, marshal(t, infoDict{Length: 100, Name: "x", PieceLength: 16, Pieces: bytes.Repeat([]byte{1}, HashSize)})),
		},
		{
			name:    "zero length content",
			torrent: torrentBytes(t, "http://t/announce", nil, marshal(t, infoDict{Length: 0, Name: "x", PieceLength: 16384, Pieces: bytes.Repeat([]byte{1}, HashSize)})),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Parse(bytes.NewReader(tt.torrent)); err == nil {
				t.Fatalf("Parse(%s) succeeded, want an error", tt.name)
			}
		})
	}
}

func TestLoadMissingFileIsAnError(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.torrent")); err == nil {
		t.Fatal("Load of a missing file succeeded")
	}
}

func TestLoadReadsARealFile(t *testing.T) {
	info := infoDict{Length: 4, Name: "x", PieceLength: 16384, Pieces: bytes.Repeat([]byte{1}, HashSize)}
	path := filepath.Join(t.TempDir(), "fixture.torrent")
	if err := os.WriteFile(path, torrentBytes(t, "http://t/announce", nil, marshal(t, info)), 0o644); err != nil {
		t.Fatal(err)
	}

	m, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if m.PieceCount() != 1 {
		t.Fatalf("PieceCount = %d, want 1", m.PieceCount())
	}
}
