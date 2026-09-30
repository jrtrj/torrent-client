// Package tracker talks to BitTorrent trackers.
//
// HTTP announce is the first transport; a UDP transport (BEP 15) plugs in
// behind the same interface later. The interface shape, the announce parameter
// set, the retry policy, and the compact-peer decoding rules are pinned by the
// tracker design ticket.
//
// Import direction: this package may import internal/bencode. Nothing below it
// may import this package.
package tracker
