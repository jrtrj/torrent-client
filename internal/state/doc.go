// Package state persists what is needed to resume a download: the pieces that
// verified, plus whatever partial-block progress the resume design keeps.
//
// The sidecar schema, its versioning, and the atomic-write scheme are pinned
// by the resume ticket.
//
// Import direction: leaf package; it imports nothing else in the tree.
package state
