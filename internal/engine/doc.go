// Package engine owns the download and upload machinery: pending-piece state,
// per-connection pumps, pipelining, integrity checking, and the statistics the
// UI consumes.
//
// It sits at the top of the internal layering and is the only package allowed
// to combine tracker, wire, and storage. Its concurrency model and failure
// policy are pinned by the piece-engine ticket.
//
// Import direction: engine may import bencode, tracker, wire, storage, and
// state. Only the CLI and internal/ui may import engine.
package engine
