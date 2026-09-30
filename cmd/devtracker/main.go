// Command devtracker is the in-repo development tracker used by the local
// swarm harness.
//
// It is a test fixture, not a product: it implements only enough of the
// announce protocol to serve the verification tests, and nothing it does
// should be treated as tracker-conformance behaviour.
//
// Not implemented yet — delivered by ticket #17.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "devtracker: not implemented yet (ticket #17)")
	os.Exit(1)
}
