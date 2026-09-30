package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// Storage is a single-file content store: the contiguous piece stream maps
// one-to-one onto the file. Multi-file torrents map onto a directory of files
// and arrive with the multi-file ticket.
type Storage struct {
	f           *os.File
	pieceLength int64
	totalLength int64
}

// Open creates or reopens path, sized to the torrent's content length. A
// missing parent directory is created.
func Open(path string, pieceLength, totalLength int64) (*Storage, error) {
	if pieceLength <= 0 {
		return nil, fmt.Errorf("storage: piece length %d is not positive", pieceLength)
	}
	if totalLength < 0 {
		return nil, fmt.Errorf("storage: content length %d is negative", totalLength)
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	// Truncating guarantees the file is exactly the torrent's length even when
	// a previous attempt left a longer or shorter one behind.
	if err := f.Truncate(totalLength); err != nil {
		f.Close()
		return nil, err
	}
	return &Storage{f: f, pieceLength: pieceLength, totalLength: totalLength}, nil
}

// Length is the content length in bytes.
func (s *Storage) Length() int64 { return s.totalLength }

// WritePiece stores one verified piece at its position in the piece stream.
func (s *Storage) WritePiece(index int, data []byte) error {
	off := int64(index) * s.pieceLength
	if index < 0 || int64(len(data)) > s.pieceLength || off+int64(len(data)) > s.totalLength {
		return fmt.Errorf("storage: piece %d (%d bytes) does not fit in piece length %d and content length %d",
			index, len(data), s.pieceLength, s.totalLength)
	}
	if _, err := s.f.WriteAt(data, off); err != nil {
		return fmt.Errorf("storage: write piece %d: %w", index, err)
	}
	return nil
}

// ReadBlock returns length bytes at begin within piece index. The caller is
// expected to have checked the range against the piece size already.
func (s *Storage) ReadBlock(index int, begin, length uint32) ([]byte, error) {
	off := int64(index)*s.pieceLength + int64(begin)
	if index < 0 || off+int64(length) > s.totalLength {
		return nil, fmt.Errorf("storage: block %d+%d of piece %d is out of range", begin, length, index)
	}
	buf := make([]byte, length)
	if _, err := s.f.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("storage: read block %d+%d of piece %d: %w", begin, length, index, err)
	}
	return buf, nil
}

// Close releases the underlying file.
func (s *Storage) Close() error { return s.f.Close() }
