package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// maxOpenFiles bounds the lazily opened file-handle cache. A multi-file
// torrent can declare thousands of files and a client that opened them all
// would run the process out of descriptors; pieces are written in roughly
// stream order, so a small cache still hits most of the time.
const maxOpenFiles = 16

// FileSpec is one file of a multi-file torrent: its path segments, relative to
// the torrent's root directory, and its length. The type is deliberately local
// to storage rather than taken from metainfo so the store stays a leaf package.
type FileSpec struct {
	Path   []string
	Length int64
}

// span is one file's slice of the contiguous piece stream: the file on disk,
// where its first byte sits in the stream, and how long it is.
type span struct {
	path   string
	start  int64
	length int64
}

// Storage is the content store.
//
// Every torrent is modelled as ONE CONTIGUOUS BYTE STREAM laid over a list of
// files, and a single-file torrent is simply the degenerate case of one span
// covering the whole stream. Unifying the two shapes here is what lets the
// download, resume and seed paths avoid caring which kind of torrent they are
// serving: a piece that straddles a file boundary is just a stream range that
// happens to cross two spans.
type Storage struct {
	pieceLength int64
	totalLength int64
	spans       []span

	mu    sync.Mutex
	open  map[int]*os.File
	order []int
}

// Open opens a single-file torrent's content at path, sized to totalLength.
// A missing parent directory is created.
func Open(path string, pieceLength, totalLength int64) (*Storage, error) {
	clean := filepath.Clean(path)
	return build([]span{{path: clean, start: 0, length: totalLength}}, pieceLength, totalLength)
}

// OpenMulti opens a multi-file torrent's content under dir. Each file's path
// segments are validated before anything touches the filesystem, so a torrent
// declaring a traversal (or an absolute or Windows-style path) is refused
// rather than allowed to write outside dir.
func OpenMulti(dir string, files []FileSpec, pieceLength, totalLength int64) (*Storage, error) {
	root := filepath.Clean(dir)
	if len(files) == 0 {
		return nil, fmt.Errorf("storage: multi-file torrent has no files")
	}

	spans := make([]span, 0, len(files))
	var off int64
	for i, f := range files {
		if err := validateSegments(f.Path); err != nil {
			return nil, fmt.Errorf("storage: file %d: %w", i, err)
		}
		if f.Length < 0 {
			return nil, fmt.Errorf("storage: file %d has negative length %d", i, f.Length)
		}
		spans = append(spans, span{
			path:   filepath.Join(append([]string{root}, f.Path...)...),
			start:  off,
			length: f.Length,
		})
		off += f.Length
	}
	if off != totalLength {
		return nil, fmt.Errorf("storage: files sum to %d bytes but the torrent says %d", off, totalLength)
	}
	return build(spans, pieceLength, totalLength)
}

// validateSegments rejects anything that could escape the output directory.
// The torrent format lets a file be named anything, including `..`, so this is
// the boundary where a hostile .torrent stops being a path.
func validateSegments(segs []string) error {
	if len(segs) == 0 {
		return fmt.Errorf("empty path")
	}
	for _, s := range segs {
		if err := validateSegment(s); err != nil {
			return fmt.Errorf("path segment %q: %w", s, err)
		}
	}
	return nil
}

func validateSegment(s string) error {
	switch {
	case s == "":
		return fmt.Errorf("empty")
	case s == "." || s == "..":
		return fmt.Errorf("relative directory reference")
	case strings.ContainsRune(s, 0):
		return fmt.Errorf("contains a NUL byte")
	case strings.ContainsAny(s, `/\`):
		return fmt.Errorf("contains a path separator")
	case strings.ContainsRune(s, ':'):
		// A drive letter ("C:evil") or an alternate data stream on Windows.
		// Plain colons are legal on unix but no legitimate torrent needs one,
		// and refusing them is cheaper than reasoning about every platform.
		return fmt.Errorf("contains a colon")
	}
	return nil
}

// ValidateName checks the torrent's own name, which for a multi-file torrent
// becomes the root directory and so must be a single safe path segment.
func ValidateName(name string) error { return validateSegment(name) }

// build creates every file up front so the store presents the full length of
// the content immediately, then hands out handles lazily.
func build(spans []span, pieceLength, totalLength int64) (*Storage, error) {
	if pieceLength <= 0 {
		return nil, fmt.Errorf("storage: piece length %d is not positive", pieceLength)
	}
	if totalLength < 0 {
		return nil, fmt.Errorf("storage: content length %d is negative", totalLength)
	}
	for _, sp := range spans {
		if dir := filepath.Dir(sp.path); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
		}
		f, err := os.OpenFile(sp.path, os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			return nil, err
		}
		// Truncating guarantees the file is exactly the declared length even
		// when an earlier attempt left a longer or shorter one behind.
		if err := f.Truncate(sp.length); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	}
	return &Storage{
		pieceLength: pieceLength,
		totalLength: totalLength,
		spans:       spans,
		open:        make(map[int]*os.File),
	}, nil
}

// Length is the content length in bytes.
func (s *Storage) Length() int64 { return s.totalLength }

// Files is the number of files holding the content: one for a single-file
// torrent, the number of entries for a multi-file one.
func (s *Storage) Files() int { return len(s.spans) }

// WritePiece stores one verified piece at its position in the piece stream,
// splitting the write across file boundaries when the piece straddles them.
func (s *Storage) WritePiece(index int, data []byte) error {
	if index < 0 || int64(len(data)) > s.pieceLength {
		return fmt.Errorf("storage: piece %d is %d bytes, which does not fit a %d-byte piece",
			index, len(data), s.pieceLength)
	}
	off := int64(index) * s.pieceLength
	if off+int64(len(data)) > s.totalLength {
		return fmt.Errorf("storage: piece %d at offset %d overruns the %d-byte content",
			index, off, s.totalLength)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeStreamLocked(off, data)
}

// ReadBlock returns length bytes at begin within piece index, gathering them
// across file boundaries when the block straddles them.
func (s *Storage) ReadBlock(index int, begin, length uint32) ([]byte, error) {
	off := int64(index)*s.pieceLength + int64(begin)
	if index < 0 || off+int64(length) > s.totalLength {
		return nil, fmt.Errorf("storage: block %d+%d of piece %d is out of range", begin, length, index)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readStreamLocked(off, int(length))
}

func (s *Storage) writeStreamLocked(off int64, data []byte) error {
	for len(data) > 0 {
		i, err := s.spanAt(off)
		if err != nil {
			return err
		}
		sp := s.spans[i]
		n := sp.start + sp.length - off
		if int64(len(data)) < n {
			n = int64(len(data))
		}
		f, err := s.fileAt(i)
		if err != nil {
			return err
		}
		if _, err := f.WriteAt(data[:n], off-sp.start); err != nil {
			return fmt.Errorf("storage: write %d bytes at %d in %s: %w", n, off-sp.start, sp.path, err)
		}
		off += n
		data = data[n:]
	}
	return nil
}

func (s *Storage) readStreamLocked(off int64, length int) ([]byte, error) {
	buf := make([]byte, 0, length)
	for length > 0 {
		i, err := s.spanAt(off)
		if err != nil {
			return nil, err
		}
		sp := s.spans[i]
		n := int(sp.start + sp.length - off)
		if length < n {
			n = length
		}
		f, err := s.fileAt(i)
		if err != nil {
			return nil, err
		}
		chunk := make([]byte, n)
		if _, err := f.ReadAt(chunk, off-sp.start); err != nil {
			return nil, fmt.Errorf("storage: read %d bytes at %d in %s: %w", n, off-sp.start, sp.path, err)
		}
		buf = append(buf, chunk...)
		off += int64(n)
		length -= n
	}
	return buf, nil
}

// spanAt finds the file holding stream offset off. Zero-length files occupy no
// bytes, so they can never be selected.
func (s *Storage) spanAt(off int64) (int, error) {
	i := sort.Search(len(s.spans), func(i int) bool {
		return s.spans[i].start+s.spans[i].length > off
	})
	if i >= len(s.spans) || s.spans[i].start > off {
		return 0, fmt.Errorf("storage: stream offset %d falls in no file", off)
	}
	return i, nil
}

// fileAt returns an open handle for span i, opening it on demand and evicting
// the oldest handle once the cache is full.
func (s *Storage) fileAt(i int) (*os.File, error) {
	if f, ok := s.open[i]; ok {
		return f, nil
	}
	if len(s.order) >= maxOpenFiles {
		oldest := s.order[0]
		s.order = s.order[1:]
		if f, ok := s.open[oldest]; ok {
			_ = f.Close()
			delete(s.open, oldest)
		}
	}
	f, err := os.OpenFile(s.spans[i].path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	s.open[i] = f
	s.order = append(s.order, i)
	return f, nil
}

// Sync flushes every open file to stable storage. The engine calls it on
// shutdown so the resume sidecar, which is written after it, can never record
// a piece whose bytes have not at least reached the kernel.
func (s *Storage) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for _, f := range s.open {
		if err := f.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Close releases every open handle. It is safe to call more than once.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for i, f := range s.open {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(s.open, i)
	}
	s.order = nil
	return firstErr
}
