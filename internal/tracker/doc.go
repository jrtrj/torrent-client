// Package tracker talks to BitTorrent trackers.
//
// The announce goes over one of two transports, picked by the announce URL's
// scheme: http(s) uses the HTTP protocol, udp:// uses the BEP 15 UDP protocol
// (CONNECT, then ANNOUNCE, with SCRAPE available). Both satisfy the same
// Tracker interface, so the engine never knows which wire protocol a swarm
// speaks. The interface shape, the announce parameter set, the retry policy,
// and the compact-peer decoding rules are pinned by the tracker design ticket.
//
// Import direction: this package may import internal/bencode. Nothing below it
// may import this package.
package tracker
