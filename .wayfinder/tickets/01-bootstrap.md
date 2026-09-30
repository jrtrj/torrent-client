## Question

Bootstrap the repo so every later ticket writes into a prepared home: fresh-repo layout, go.mod, CLI skeleton, Go-version decision, and commit conventions. (Ordering: this runs before/next-to the protocol tickets — tickets' outputs are typed against code that will exist.)

1. **Layout**: `cmd/torrent-client` (main CLI) + `cmd/devtracker` (the in-repo dev tracker for the test harness, per the decided posture) + `cmd/seed` (demo seeder for the local swarm) + `internal/{bencode→if we keep bencode-go, tracker/, wire/, engine/, storage/, state/, ui/}` — or flat package given the repo size? Pick, justify; package names must make import cycles impossible (dependencies flow: ui/engine → wire/tracker, never back).
2. **Go version + module**: go.mod says 1.27.0. Confirm toolchain on this machine builds it, keep `go 1.27.0`, require jackpal/bencode-go (latest pinned).
3. **CLI skeleton protocol** (the flag grammar the spec must document): `torrent-client <torrent|magnet-uri> <output-path> [-port N] [-seed] [-max-down-rate] [-max-up-rate]` — decide subcommand-per-mode or flag-per-mode; first draft of flags now, extended by the bonus tickets; exit codes (0=success, 1=fatal, 2=usage error).
4. **Logging posture**: the reference uses `log.Printf` to stderr; with the dashboard at stdout — decide each stream's fate (what goes where), and how worker events flow to the terminal without interleaving with the dashboard line (the dashboard ticket's event-line mechanism consumes this).
5. **CI/dev-loop**: `Makefile` targets (build/test/lint/verify) — lint = `go vet` only? or staticcheck (a tool, allowed — it's not a code dep)? minimum bar: `go build ./...`, `go test ./...`, `go vet ./...` green.
6. **README skeleton**: user-facing setup/build/run of the fresh client; the research references from doc/research/ stay unlinked from it (internal docs).

## Resolution contract

- the layout decision (flat vs package tree) + import-graph rule
- go.mod final contents (module name resolved — 'torrent-client' or a fully-qualified path; one is right for a public repo)
- documented CLI first-draft (flags, exit codes) + logging posture
- Makefile targets list + CI bar
