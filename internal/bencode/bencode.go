// Package bencode wraps the bencode codec used by .torrent metainfo, tracker
// responses, and peer messages.
//
// github.com/jackpal/bencode-go is the one third-party module this project
// allows. Wrapping it here keeps that fact in a single place: the rest of the
// tree imports this package and never the module directly. The codec's
// round-trip behaviour — in particular that the raw info dictionary must
// survive decoding so the info-hash stays exact — is pinned by the bencode
// research ticket.
package bencode

import (
	"io"

	codec "github.com/jackpal/bencode-go"
)

// RawMessage is a bencoded fragment kept as its original bytes. Capturing the
// info dictionary this way is what makes an info-hash reproducible.
type RawMessage = codec.RawMessage

// Decode reads a single bencoded value from r.
func Decode(r io.Reader) (any, error) { return codec.Decode(r) }

// Unmarshal reads a bencoded value from r into v.
func Unmarshal(r io.Reader, v any) error { return codec.Unmarshal(r, v) }

// Marshal writes v to w as bencode.
func Marshal(w io.Writer, v any) error { return codec.Marshal(w, v) }
