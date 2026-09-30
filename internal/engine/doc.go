// Package engine owns the download machinery: pending-piece state, the
// per-connection pumps, block pipelining, integrity checking, and the
// statistics the UI consumes.
//
// Concurrency model: one scheduler goroutine owns all scheduling state — which
// piece a peer is working on, which blocks are in flight, who is blacklisted,
// the counters. Each connection runs its own read and write pumps and talks to
// the scheduler over channels. Because a block is claimed for exactly one peer
// at a time and a piece has exactly one owner, two workers can never fetch the
// same block; because a peer is only ever offered pieces its bitfield says it
// holds, a peer that has nothing cannot be re-enqueued in a spin.
//
// It sits at the top of the internal layering: the download half of the
// client, and the only package that combines tracker, wire, and storage. The
// upload half is internal/seed, which combines wire and storage but knows
// nothing about swarms or trackers.
//
// Import direction: engine may import bencode, tracker, wire, storage, and
// state. Only the CLI and internal/ui may import engine.
package engine
