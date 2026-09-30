// Command torrent-client is the entry point of the from-scratch BitTorrent
// client. It hands the raw arguments to run, which owns the CLI contract.
package main

import "os"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
