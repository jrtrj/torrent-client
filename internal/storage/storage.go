package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// maxOpenFiles caps the lazily opened handle cache. A torrent can declare
// thousands of files and opening all of them would run us out of
// descriptors; pieces arrive in stream order, so a small cache still hits
// most of the time.
const maxOpenFiles = 16

// FileSpec is one file of a multi-file torrent: path segments relative to the
// torrent's root, and its length. Deliberately a local mirror of metainfo
// instead of an import of it, so storage stays a leaf package.
type FileSpec struct {
	Path   []string
	Length int64
}

// span is one file's slice of the contiguous stream: where it sits on disk,
// where its first byte lands in the stream, and how long it is.
type span struct {
	path   string
	start  int64
	length int64
}

// Storage is the content store.
//
// Every torrent is modelled as ONE CONTIGUOUS BYTE STREAM laid over a list of
// files, and a single-file torrent is just the degenerate case: one span over
// the whole thing. Unifying the two here is what lets the download, resume and
// seed paths stop caring which shape they're serving — a piece straddling a
// file boundary is only a stream range that crosses two spans.
type Storage struct {
	pieceLength int64
	totalLength int64
	spans       []span

	mu    sync.Mutex
	open  map[int]*os.File
	order []int
}

// Open opens a single-file torrent's content at path, sized to totalLength.
// Missing parent directories get created.
func Open(path string, pieceLength, totalLength int64) (*Storage, error) {
	clean := filepath.Clean(path)
	return build([]span{{path: clean, start: 0, length: totalLength}}, pieceLength, totalLength)
}

// OpenMulti opens a multi-file torrent's content under dir. Every path segment
// is validated before anything touches the filesystem, so a torrent naming a
// traversal, an absolute path or a Windows-style path gets refused instead of
// writing outside dir.
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
// A torrent can name a file anything, `..` included, so this is the boundary
// where a hostile .torrent stops being a path.
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
		// A drive letter ("C:evil") or an NTFS alternate data stream. Colons
		// are legal on unix and no legitimate torrent needs one, so refusing
		// them everywhere beats reasoning about per-platform rules.
		return fmt.Errorf("contains a colon")
	}
	return nil
}

// ValidateName checks the torrent's own name. For a multi-file torrent that
// name becomes the root directory, so it has to be one safe path segment.
func ValidateName(name string) error { return validateSegment(name) }

// build creates every file up front so the store reports the full content
// length immediately; handles come later, on demand.
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
		// Truncate so the length is exactly as declared, even if an earlier
		// attempt left a longer or shorter file behind.
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

// Length is the content's total size in bytes.
func (s *Storage) Length() int64 { return s.totalLength }

// Files is how many files hold the content: one for a single-file torrent,
// the file count for a multi-file one.
func (s *Storage) Files() int { return len(s.spans) }

// WritePiece stores one verified piece at its slot in the piece stream,
// splitting the write when the piece straddles a file boundary.
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

// ReadBlock returns length bytes starting at begin inside piece index,
// stitching them together when the block straddles a file boundary.
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

// spanAt finds the file holding stream offset off. Zero-length files take up
// no bytes, so the search can never land on one.
func (s *Storage) spanAt(off int64) (int, error) {
	i := sort.Search(len(s.spans), func(i int) bool {
		return s.spans[i].start+s.spans[i].length > off
	})
	if i >= len(s.spans) || s.spans[i].start > off {
		return 0, fmt.Errorf("storage: stream offset %d falls in no file", off)
	}
	return i, nil
}

// fileAt returns an open handle for span i, opening on demand and evicting the
// oldest entry once the cache is full.
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
// shutdown, before writing the resume sidecar, so the sidecar can never
// record a piece whose bytes haven't at least reached the kernel.
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

// Close releases every open handle. Safe to call more than once.
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
