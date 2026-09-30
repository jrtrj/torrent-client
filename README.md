# torrent-client (fresh build)

A BitTorrent client written from scratch in Go — pure stdlib, zero external modules.
This is **not** a fork or continuation of `TatHack-Tathva/torrent-client`; that repo was
consulted only for requirements and expected behavior (see `docs/task-brief-*.md`).

## Status: planning

This repo currently hosts the **Wayfinder map** (GitHub Issues) that resolves every
open decision into `docs/build-spec.md` — the consolidated build spec. Implementation
starts once the map closes.

## Artifacts

- **Map** — https://github.com/jrtrj/torrent-client/issues/<map-issue> (label `wayfinder:map`)
- **Build spec** — `docs/build-spec.md` (written when the map closes)

## Tech posture

- Go 1.27 (per go.mod), standard library only — bencode codec, tracker clients,
  wire protocol, and dashboard are all hand-written.
- Features: core debug/rewrite of the protocol stack (BEP 0003), live terminal
  dashboard, resumable downloads, seeding/upload handler.
- Sequenced bonus features: BEP 0009 (magnet + ut_metadata) → BEP 0015 (UDP tracker)
  → multi-file torrents → token-bucket bandwidth limiting.
- Verified against a self-seeded localhost swarm (in-repo `cmd/devtracker` + seeder)
  and a real public torrent.
