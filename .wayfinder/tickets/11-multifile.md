## Question

Design multi-file torrent support — parse and download torrents whose `info` has a `files` list (directories of files) rather than a single `length`.

1. **Metainfo shape**: the multi-file info dict (`files: [{length, path (list of path segments), md5sum?}]`, `name` as directory name, optional `attributes`); how `piece hashes` still span the CONTIGUOUS byte stream: pieces cross file boundaries — a piece may cover the tail of one file and the head of the next.
2. **Byte→file mapping**: from the piece index → the range of bytes in the virtual concatenated stream; from a stream range → (file, offset) pairs — the boundary-spanning block read/write math (an implementation-shaped helper: `resolve(stream_offset) -> (file_idx, file_offset)`, and its inverse).
3. **Storage layer**: directory creation (mkdir -p semantics under output dir), path-join safety (a torrent declaring `path: ["..", "evil"]` or absolute/Windows-style segments — the malware vector: reject or sanitize, and say what we do with empty segments), sparse-ish sequential write pattern vs random-access writes (write-ahead to the tail piece?). File handle strategy: cache open handles? open-per-write?
4. **Resume integration**: the resume sidecar from the earlier ticket now tracks pieces that span two+ files (piece-complete requires all spanned bytes verified) — decide how the resume flow validates when one file exists and its neighbor is missing; and for the SEED side: serving must read from the right file too.
5. **Single-file unification**: after this, single-file torrents become a degenerate case (one file, no path list): pick whether we unify (all torrents = virtual byte stream + file map) — this is the strong-architecture answer, and the seed ticket's read path also depends on it. CLI shape: `torrent-client multi.torrent outdir/`.

Depends on: bencode ticket (how `files` arrives through bencode-go), resume ticket (sidecar schema extension), seed ticket (read-at-request path).

## Resolution contract

- the info-dict struct additions + parse validation rules (incl. the path traversal defense)
- the virtual byte-stream mapping helpers with signatures + boundary-span behavior (block write/read spanning files)
- storage layout decisions (dir creation, handle strategy)
- resume + seed integration changes (what the sidecar gains, what the serving read path gains)
