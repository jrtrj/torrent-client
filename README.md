# torrent-client (fresh build)

A BitTorrent client written from scratch in Go — the standard library everywhere
except the bencode codec.
This is **not** a fork or continuation of `TatHack-Tathva/torrent-client`; that repo was
consulted only for requirements and expected behavior (see `docs/task-brief-*.md`).

## Status: planning (walking skeleton in place)

The **Wayfinder map** (GitHub Issues) is still resolving the open design decisions into
`docs/build-spec.md` — the consolidated build spec. The repository skeleton and the
walking skeleton now exist: the client downloads a **single-file** torrent end to end
from the local swarm (metainfo → announce → handshake → sequential piece fetch → SHA-1
verify → write to disk). Not implemented yet: multi-file torrents, magnet links,
resuming, the live dashboard, client-side seeding, UDP trackers, and rate limiting.

## Artifacts

- **Map** — https://github.com/jrtrj/torrent-client/issues/<map-issue> (label `wayfinder:map`)
- **Build spec** — `docs/build-spec.md` (written when the map closes)

## Build and run

Requires Go 1.27 or newer.

```sh
make build     # go build ./...
make test      # go test ./...
make verify    # the CI bar: build + vet + test + gofmt check
```

```sh
torrent-client [flags] <torrent-file | magnet-uri> <output-path>
```

Flags: `-port` (default 6881), `-seed`, `-max-down-rate`, `-max-up-rate` (for example
`512k` or `2M`; omitted means unlimited). Flags must come before the two positional
arguments. Run `torrent-client -h` for the full list.

Exit codes: `0` success, `1` fatal error, `2` usage error.

## Local swarm (development)

No public tracker is needed to exercise the client. The repo ships a test tracker and a
seeder that speak the real protocols over loopback:

```sh
go run ./cmd/devtracker -addr 127.0.0.1:9998   # prints the address it bound
go run ./cmd/seed <torrent-file> <data-path>   # announces, then serves the data
go run ./cmd/torrent-client <torrent-file> <output-path>
```

`devtracker` flags: `-addr` (default `127.0.0.1:9998`), `-interval` in seconds (default 5).
`seed` flags: `-tracker` (defaults to the torrent's own announce URL), `-port` (0 picks a
free port), `-listen`. The torrent's announce URL must point at the dev tracker.

Both are test fixtures, not products — they implement only what the local tests need.
The end-to-end test generates the fixture data and its `.torrent` programmatically and
runs the whole swarm on ephemeral ports, so `go test ./cmd/torrent-client/ -run EndToEnd -v`
verifies the path without any manual setup.

## Tech posture

- Go 1.27 (per go.mod). Hand-rolled on the standard library everywhere except the bencode
  codec, which may use `github.com/jackpal/bencode-go` — the only third-party module
  (user decision after initial zero-dependency plan).
- Features: core debug/rewrite of the protocol stack (BEP 0003), live terminal
  dashboard, resumable downloads, seeding/upload handler.
- Sequenced bonus features: BEP 0009 (magnet + ut_metadata) → BEP 0015 (UDP tracker)
  → multi-file torrents → token-bucket bandwidth limiting.
- Verified against a self-seeded localhost swarm (in-repo `cmd/devtracker` + seeder)
  and a real public torrent.
