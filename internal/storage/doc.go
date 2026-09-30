// Package storage reads and writes torrent content on disk.
//
// For multi-file torrents it owns the mapping between offsets in the
// contiguous piece stream and (file, offset) pairs. The mapping helpers and
// the path-traversal defenses are pinned by the multi-file ticket.
//
// Import direction: leaf package; it imports nothing else in the tree.
package storage
