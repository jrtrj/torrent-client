# torrent-client (fresh build)

A BitTorrent client written from scratch in Go — the standard library everywhere
except the bencode codec.
This is **not** a fork or continuation of `TatHack-Tathva/torrent-client`; that repo was
consulted only for requirements and expected behavior (see `docs/task-brief-*.md`).

## Status: feature-complete for the planned scope

The build tickets (#16–#26) are implemented and verified against a self-seeded localhost
swarm: the client downloads a torrent end to end (a `.torrent` file or a magnet link →
announce over HTTP or UDP → handshake → concurrent piece fetch → SHA-1 verify → write to
disk), resumes an interrupted download from a sidecar, seeds back to the swarm, serves the
metadata to magnet peers, and reports a live terminal dashboard — with token-bucket caps on
both directions.

Out of scope, deliberately: DHT and PEX (so a magnet link must carry a tracker), peer
encryption, uTP, and superseeding. A magnet URI with no `tr=` is refused up front rather
than accepted and later found unresolvable.

## Artifacts

- **Map** — [#1](https://github.com/jrtrj/torrent-client/issues/1) (label `wayfinder:map`)
- **Build spec** — [#15](https://github.com/jrtrj/torrent-client/issues/15) — the current synthesis;
  `docs/build-spec.md` supersedes it when the map closes
- **Build tickets** — [#16–#26](https://github.com/jrtrj/torrent-client/issues?q=is%3Aissue+label%3Aready-for-agent)
  (label `ready-for-agent`)

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
`512k` or `2M`; omitted means unlimited). The two rate caps are enforced in both
directions: `-max-down-rate` paces the bytes the client accepts, `-max-up-rate` paces the
bytes it serves (it needs `-seed` to have anything to serve). Flags must come before the
two positional arguments. Run `torrent-client -h` for the full list.

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

The magnet path needs no `.torrent` file at all — the seeder serves the metadata over
ut_metadata too, so the client can be handed nothing but an info-hash and a tracker:

```sh
go run ./cmd/torrent-client \
  "magnet:?xt=urn:btih:<info-hash>&tr=http://127.0.0.1:9998/announce" <output-path>
```

Both are test fixtures, not products — they implement only what the local tests need.
The end-to-end test generates the fixture data and its `.torrent` programmatically and
runs the whole swarm on ephemeral ports, so `go test ./cmd/torrent-client/ -run EndToEnd -v`
verifies the path without any manual setup.

## Tech posture

- Go 1.27 (per go.mod). Hand-rolled on the standard library everywhere except the bencode
  codec, which may use `github.com/jackpal/bencode-go` — the only third-party module
  (user decision after initial zero-dependency plan).
- Features: the peer protocol (BEP 0003), single- and multi-file torrents, HTTP and UDP
  (BEP 0015) trackers, magnet links resolved over ut_metadata (BEP 0009), resumable
  downloads, a seeding/upload handler that serves the metadata as well as the content, a
  live terminal dashboard, and token-bucket bandwidth limiting per direction.
- Not implemented: DHT (BEP 0005), PEX, peer encryption, uTP, and superseeding.
- Verified against a self-seeded localhost swarm (in-repo `cmd/devtracker` + seeder)
  and a real public torrent.
