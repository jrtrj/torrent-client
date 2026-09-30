// Package wire implements the peer wire protocol (BEP 3): the handshake frame
// and the length-prefixed message set.
//
// The on-the-wire contract is renegotiated rather than inherited, so the exact
// frame layout, the bounds checks, and the timeout minimums live with the wire
// protocol ticket.
//
// Import direction: this package may import internal/bencode. Nothing below it
// may import this package.
package wire
