// Package state persists what is needed to resume a download: the pieces that
// have been hash-verified and written, as a sidecar file beside the output.
//
// # Keying rule
//
// A sidecar belongs to exactly one (torrent, output) pair. Its path is
// "<output>.resume", and the record inside repeats both halves of the identity:
// the torrent's info-hash (hex) and the cleaned output path. Loading therefore
// requires both to match, in addition to the schema version and the torrent
// geometry (piece length, total length, piece count). Two torrents, or two
// output files, can never collide: a foreign or stale sidecar is reported as
// ErrMismatch and treated exactly like an absent one, so the download starts
// fresh rather than adopting bytes that were never verified for this file.
//
// # Versioning
//
// Every record carries SchemaVersion. The layout may change between versions,
// so a sidecar from a different version is rejected instead of guessed at.
//
// # Granularity
//
// The record stores verified pieces only, not partial-block progress. An
// interrupted run therefore re-fetches at most the pieces that were still
// assembling when it died — bounded by how many pieces the scheduler had handed
// out (one per active peer), never by the torrent's size. Per-block salvage
// would buy back those few pieces' worth of blocks at the cost of a block-level
// record written on every block, and the engine discards a piece's partial
// blocks anyway when a contributing peer leaves or a hash fails, so a
// block-level record could not be trusted without the same re-hashing on disk.
// Piece granularity keeps the sidecar small and recovery simple, and what it
// costs to re-fetch is bounded by concurrency rather than by the download.
//
// # Atomic writes
//
// Save writes a temp file in the sidecar's own directory, fsyncs it, renames
// it over the sidecar, and fsyncs the directory. A crash can therefore never
// leave a half-written sidecar that a later run would trust: a reader observes
// a whole sidecar, either the previous one or the new one.
//
// The package is a leaf: it imports nothing else in the tree. Deciding which
// claimed pieces still hash correctly on disk is the engine's job, because it
// owns the content store the check reads.
package state
