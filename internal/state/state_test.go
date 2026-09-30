package state

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func bitfield(pieces int, set ...int) []byte {
	b := make([]byte, (pieces+7)/8)
	for _, i := range set {
		b[i/8] |= 0x80 >> uint(i%8)
	}
	return b
}

func TestSaveLoadRoundTrip(t *testing.T) {
	const pieces = 11
	out := filepath.Join(t.TempDir(), "content.bin")
	var hash [20]byte
	copy(hash[:], "info-hash-for-the-round-trip")
	st := Open(out, hash, 4096, int64(pieces)*4096, pieces)

	if _, err := st.Load(); !errors.Is(err, ErrNoState) {
		t.Fatalf("Load on a fresh store = %v, want ErrNoState", err)
	}

	have := bitfield(pieces, 0, 3, 7, 10)
	if err := st.Save(have); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := st.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Version != SchemaVersion {
		t.Fatalf("version = %d, want %d", got.Version, SchemaVersion)
	}
	if got.Pieces != pieces || got.TotalLength != int64(pieces)*4096 || got.PieceLength != 4096 {
		t.Fatalf("geometry = %d/%d/%d, want %d/4096/%d", got.Pieces, got.PieceLength, got.TotalLength, pieces, int64(pieces)*4096)
	}
	if string(got.Have) != string(have) {
		t.Fatalf("bitfield = %v, want %v", got.Have, have)
	}
	if err := st.Remove(); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := st.Remove(); err != nil {
		t.Fatalf("Remove is not idempotent: %v", err)
	}
	if _, err := st.Load(); !errors.Is(err, ErrNoState) {
		t.Fatalf("Load after Remove = %v, want ErrNoState", err)
	}
}

// A sidecar carries both halves of its key. A copy sitting at another output
// path, or a record naming another torrent, must be refused rather than
// adopted — that's the collision the keying rule exists to prevent.
func TestLoadRejectsMismatchedIdentity(t *testing.T) {
	const pieces = 5
	dir := t.TempDir()
	outA := filepath.Join(dir, "a.bin")
	outB := filepath.Join(dir, "b.bin")
	var hashA, hashB [20]byte
	hashA[0], hashB[0] = 0x11, 0x22

	a := Open(outA, hashA, 4096, int64(pieces)*4096, pieces)
	if err := a.Save(bitfield(pieces, 1)); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Same output path, different torrent:
	if _, err := Open(outA, hashB, 4096, int64(pieces)*4096, pieces).Load(); !errors.Is(err, ErrMismatch) {
		t.Fatalf("Load with a different info-hash = %v, want ErrMismatch", err)
	}
	// Different geometry:
	if _, err := Open(outA, hashA, 8192, int64(pieces)*8192, pieces).Load(); !errors.Is(err, ErrMismatch) {
		t.Fatalf("Load with a different piece length = %v, want ErrMismatch", err)
	}
	// Same torrent, a sidecar copied to another output path:
	raw, err := os.ReadFile(a.Path())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(SidecarPath(outB), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(outB, hashA, 4096, int64(pieces)*4096, pieces).Load(); !errors.Is(err, ErrMismatch) {
		t.Fatalf("Load of a sidecar copied to another output = %v, want ErrMismatch", err)
	}
}

func TestLoadRejectsCorruptOrStaleRecords(t *testing.T) {
	const pieces = 5
	dir := t.TempDir()
	out := filepath.Join(dir, "content.bin")
	var hash [20]byte
	hash[0] = 0x7c
	st := Open(out, hash, 4096, int64(pieces)*4096, pieces)

	cases := []struct {
		name string
		data string
	}{
		{"not json", "{ this is not a sidecar"},
		{"truncated json", `{"version":1,"info_hash":"7c`},
		{"empty", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(st.Path(), []byte(tc.data), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := st.Load(); !errors.Is(err, ErrMismatch) {
				t.Fatalf("Load(%s) = %v, want ErrMismatch", tc.name, err)
			}
		})
	}

	// A record from a future schema must be refused, not guessed at.
	write := func(v any) {
		t.Helper()
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(st.Path(), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	base := State{
		Version:     SchemaVersion + 1,
		InfoHash:    "7c00000000000000000000000000000000000000",
		Output:      out,
		PieceLength: 4096,
		TotalLength: int64(pieces) * 4096,
		Pieces:      pieces,
		Have:        bitfield(pieces),
	}
	write(base)
	if _, err := st.Load(); !errors.Is(err, ErrMismatch) {
		t.Fatalf("Load of a future schema version = %v, want ErrMismatch", err)
	}

	// A bitfield of the wrong length is a layout disagreement, not a record.
	base.Version = SchemaVersion
	base.Have = bitfield(pieces + 8)
	write(base)
	if _, err := st.Load(); !errors.Is(err, ErrMismatch) {
		t.Fatalf("Load with a wrong-length bitfield = %v, want ErrMismatch", err)
	}

	// Set padding bits are part of no piece and must be refused.
	base.Have = bitfield(pieces)
	base.Have[len(base.Have)-1] |= 0x01
	write(base)
	if _, err := st.Load(); !errors.Is(err, ErrMismatch) {
		t.Fatalf("Load with padding bits set = %v, want ErrMismatch", err)
	}
}

// TestSaveIsAtomic hammers Save from one goroutine while another reads: a
// reader must only ever observe a whole sidecar, never a partial one. That's
// the property the temp-file-plus-rename dance buys.
func TestSaveIsAtomic(t *testing.T) {
	const pieces = 13
	out := filepath.Join(t.TempDir(), "content.bin")
	var hash [20]byte
	hash[0] = 0x5a
	st := Open(out, hash, 4096, int64(pieces)*4096, pieces)
	if err := st.Save(bitfield(pieces, 0)); err != nil {
		t.Fatalf("initial Save: %v", err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var werr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 1; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			var set []int
			for i := 0; i < pieces; i++ {
				if (i+n)%2 == 0 {
					set = append(set, i)
				}
			}
			if err := st.Save(bitfield(pieces, set...)); err != nil {
				werr = err
				return
			}
		}
	}()

	for i := 0; i < 3000; i++ {
		got, err := st.Load()
		if err != nil {
			t.Fatalf("read %d observed a bad sidecar: %v (a partial write was visible)", i, err)
		}
		if len(got.Have) != (pieces+7)/8 {
			t.Fatalf("read %d: bitfield is %d bytes, want %d", i, len(got.Have), (pieces+7)/8)
		}
	}
	close(stop)
	wg.Wait()
	if werr != nil {
		t.Fatalf("writer failed: %v", werr)
	}

	// No temp file may be left behind.
	entries, err := os.ReadDir(filepath.Dir(out))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if name := e.Name(); name != filepath.Base(st.Path()) {
			t.Fatalf("stray file left in the sidecar directory: %s", name)
		}
	}
}

func TestSaveRejectsWrongBitfieldLength(t *testing.T) {
	out := filepath.Join(t.TempDir(), "content.bin")
	var hash [20]byte
	st := Open(out, hash, 4096, 10*4096, 10)
	if err := st.Save(bitfield(4)); err == nil {
		t.Fatal("Save accepted a bitfield that does not match the piece count")
	}
}

func TestSidecarPathIsCleaned(t *testing.T) {
	if got, want := SidecarPath("./out.bin"), "out.bin"+SidecarSuffix; got != want {
		t.Fatalf("SidecarPath = %q, want %q", got, want)
	}
}
