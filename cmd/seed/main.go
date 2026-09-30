// Command seed is the demo seeder for the local swarm: it holds a fixture and
// serves it over the real peer protocol so a download can be verified offline.
//
// Not implemented yet — delivered by ticket #17.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "seed: not implemented yet (ticket #17)")
	os.Exit(1)
}
