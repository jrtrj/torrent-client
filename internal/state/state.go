package state

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// SidecarSuffix is appended to the output path to name the resume sidecar. The
// sidecar therefore always sits next to the content it describes and can never
// be mistaken for the content itself.
const SidecarSuffix = ".resume"

// SchemaVersion is the on-disk format version, written into every sidecar and
// required to match on load. The layout may change between versions, so a
// sidecar from another version is rejected rather than guessed at: a wrong
// guess could adopt bytes that were never verified.
const SchemaVersion = 1

var (
	// ErrNoState means there is no sidecar for this output: a fresh download.
	ErrNoState = errors.New("state: no resume sidecar")
	// ErrMismatch means a sidecar exists but does not describe this download:
	// another torrent, another output path, another schema version, a
	// different torrent geometry, or bytes that are not a valid sidecar at
	// all. It must never be trusted, so callers treat it exactly like
	// ErrNoState and start fresh.
	ErrMismatch = errors.New("state: resume sidecar does not match this download")
)

// State is one persisted resume record. It carries both halves of the key —
// the torrent info-hash and the output path — so a sidecar that was copied,
// renamed, or left over from a previous torrent at the same path is detected
// as foreign instead of adopted.
type State struct {
	Version     int    `json:"version"`
	InfoHash    string `json:"info_hash"` // lower-case hex of the 20-byte hash
	Output      string `json:"output"`
	PieceLength int64  `json:"piece_length"`
	TotalLength int64  `json:"total_length"`
	Pieces      int    `json:"pieces"`
	// Have is the verified-piece bitfield: one bit per piece, most
	// significant bit first within each byte, padding bits zero. JSON
	// renders it as base64.
	Have []byte `json:"have"`
}

// Store is the resume sidecar for one (torrent, output) pair. It performs no
// I/O at construction; Load and Save do. Methods are not internally
// synchronised: the engine serialises its calls, and the atomic-write test
// shows a concurrent reader only ever sees a whole sidecar.
type Store struct {
	path        string
	infoHash    [20]byte
	output      string
	pieceLength int64
	totalLength int64
	pieces      int
}

// SidecarPath is where a given output path keeps its resume sidecar. The path
// is cleaned so two spellings of the same file ("./out" and "out") share one
// sidecar.
func SidecarPath(output string) string { return filepath.Clean(output) + SidecarSuffix }

// Open returns the sidecar handle for output under the given torrent geometry.
func Open(output string, infoHash [20]byte, pieceLength, totalLength int64, pieces int) *Store {
	output = filepath.Clean(output)
	return &Store{
		path:        output + SidecarSuffix,
		infoHash:    infoHash,
		output:      output,
		pieceLength: pieceLength,
		totalLength: totalLength,
		pieces:      pieces,
	}
}

// Path is the sidecar's file path.
func (s *Store) Path() string { return s.path }

// Load reads and validates the sidecar. ErrNoState means there is none;
// ErrMismatch means there is one but it describes something else, including
// the case where it cannot be parsed. Callers treat both as "start fresh".
func (s *Store) Load() (*State, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNoState
		}
		return nil, fmt.Errorf("state: read %s: %w", s.path, err)
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("%w: %s is not valid JSON: %v", ErrMismatch, s.path, err)
	}
	if err := s.check(&st); err != nil {
		return nil, err
	}
	return &st, nil
}

// check enforces the keying and geometry rules. Every failure is ErrMismatch:
// the file is not a resume record we are willing to trust.
func (s *Store) check(st *State) error {
	if st.Version != SchemaVersion {
		return fmt.Errorf("%w: schema version %d, want %d", ErrMismatch, st.Version, SchemaVersion)
	}
	if want := hex.EncodeToString(s.infoHash[:]); st.InfoHash != want {
		return fmt.Errorf("%w: info-hash %s, want %s", ErrMismatch, st.InfoHash, want)
	}
	if st.Output != s.output {
		return fmt.Errorf("%w: output %q, want %q", ErrMismatch, st.Output, s.output)
	}
	if st.PieceLength != s.pieceLength || st.TotalLength != s.totalLength || st.Pieces != s.pieces {
		return fmt.Errorf("%w: geometry piece length %d total %d pieces %d, want %d/%d/%d",
			ErrMismatch, st.PieceLength, st.TotalLength, st.Pieces, s.pieceLength, s.totalLength, s.pieces)
	}
	want := (s.pieces + 7) / 8
	if len(st.Have) != want {
		return fmt.Errorf("%w: bitfield is %d bytes, want %d for %d pieces", ErrMismatch, len(st.Have), want, s.pieces)
	}
	// Pad bits past the last piece are part of the frame, not of the record;
	// a set one means the writer and this reader disagree about the layout.
	if pad := s.pieces % 8; pad != 0 && len(st.Have) > 0 {
		if mask := byte(1<<(8-pad) - 1); st.Have[len(st.Have)-1]&mask != 0 {
			return fmt.Errorf("%w: padding bits set past the last piece", ErrMismatch)
		}
	}
	return nil
}

// Save writes the verified-piece bitfield atomically: a temp file in the same
// directory is written, fsynced and closed, then renamed over the sidecar, and
// the directory is synced last. A reader therefore only ever sees a whole,
// old-or-new sidecar, and a crash mid-write leaves the previous one intact.
func (s *Store) Save(have []byte) error {
	if want := (s.pieces + 7) / 8; len(have) != want {
		return fmt.Errorf("state: bitfield is %d bytes, want %d for %d pieces", len(have), want, s.pieces)
	}
	st := State{
		Version:     SchemaVersion,
		InfoHash:    hex.EncodeToString(s.infoHash[:]),
		Output:      s.output,
		PieceLength: s.pieceLength,
		TotalLength: s.totalLength,
		Pieces:      s.pieces,
		Have:        append([]byte(nil), have...),
	}
	data, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("state: marshal: %w", err)
	}
	data = append(data, '\n')

	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, filepath.Base(s.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("state: create temp sidecar: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("state: write temp sidecar: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("state: fsync temp sidecar: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("state: close temp sidecar: %w", err)
	}
	// Rename is the atomic step: up to here the real sidecar is untouched.
	if err := os.Rename(tmpName, s.path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("state: rename sidecar into place: %w", err)
	}
	// Sync the directory so the new name is durable. Without this a crash
	// could lose the rename and leave the older sidecar, which is safe but
	// would silently resume less than we reported.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Remove deletes the sidecar. A missing sidecar is not an error, so removing it
// twice is fine.
func (s *Store) Remove() error {
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("state: remove %s: %w", s.path, err)
	}
	return nil
}
