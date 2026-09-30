# torrent-client (fresh build)

A BitTorrent client written from scratch in Go — the standard library everywhere
except the bencode codec.
This is **not** a fork or continuation of `TatHack-Tathva/torrent-client`; that repo was
consulted only for requirements and expected behavior (see `docs/task-brief-*.md`).

## Status: planning (skeleton in place)

The **Wayfinder map** (GitHub Issues) is still resolving the open design decisions into
`docs/build-spec.md` — the consolidated build spec. The repository skeleton now exists
(package layout, CLI grammar, dev-loop) so the build tickets write into a prepared home.
The download pipeline itself is not implemented yet.

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
