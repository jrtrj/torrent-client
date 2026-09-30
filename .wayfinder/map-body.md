## Destination

A build-ready engineering spec for a fresh, from-scratch Go BitTorrent client (pure stdlib, no external modules) — consolidated as `docs/build-spec.md` in jrtrj/torrent-client. The reference repo (TatHack-Tathva/torrent-client) is consulted for requirements/behavior only, never copied. The spec must resolve, for every subsystem (bencode, tracker, wire protocol, piece pipeline, dashboard, resumability, seeding, magnets/ut_metadata, UDP tracker, multi-file, bandwidth limiting), the design, the interface, and how it is verified — leaving no decision open before implementation sessions begin. Bonus features are sequenced: BEP 0009 first, then BEP 0015, multi-file, rate limiting.

## Notes

- Domain: BitTorrent protocol engineering (BEP 3 core + hand-rolled protocol plumbing). Skills to consult per session: "grilling" + "domain-modeling" by default; research tickets spawn subagents with the full context in their body (subagents see nothing else).
- Plan-first: this map resolves decisions and produces the spec; it does NOT implement. Implementation happens in later sessions reading the closed tickets' resolution comments.
- Fresh implementation posture: every subsystem hand-rolled on the Go standard library, with exactly one exception decided by the user — the bencode codec may use github.com/jackpal/bencode-go (the library the reference repo used). Tracker clients, wire protocol, engine, dashboard: pure stdlib. Go 1.27 per go.mod.
- Human factors: Jerit reviews commits (agent writes code in build sessions — deliverable mode, not guidance mode). No deadline pressure. Grilling uses concrete examples (speed math with real numbers) before abstract rules.
- Verification posture (decided): self-seeded localhost swarm (we ship a minimal dev tracker under cmd/devtracker that the harness boots, plus a demo seeder) AND at least one real public torrent; dashboard = single-line in-place ANSI updater (bar + % + speed + ETA + workers, other events logged below the line); final spec = docs/build-spec.md.
- Scope pacing: no deadline pressure — quality over speed; never resolve more than one ticket per session (research tickets excepted).
- Magnet scope (decided): tracker-bearing magnets only — parse trackers, fetch metadata from peers via ut_metadata (BEP 9). DHT explicitly out of scope.
- Convention: the map is an index, not a store — decisions live in ticket resolution comments; the map only gists + links.

## Decisions so far

<!-- filled as tickets close: one line each — gist only, link the ticket -->

## Not yet specified

- Piece Management: exact endgame mode policy (when/how many workers race final pieces), optimistic-unchoke behavior for uploads, keep-alive timeout values — shaped once the wire-protocol and pipeline decisions land.
- Resume-state details: file layout (sidecar vs inline), partial-block persistence policy, atomic-write scheme — shaped once resumability design starts.
- ut_metadata UX details: timeout budget per metadata attempt, peer fallback order — shaped once BEP 9 research lands.
- UDP tracker: connection-id lifecycle details and retries — shaped once BEP 15 is reached.
- Bandwidth shaping: per-direction tokens vs global, burst policy — shaped once core pipeline is stable.

## Out of scope

<!-- work consciously ruled beyond this destination; returns only if the destination is redrawn -->

- DHT / trackerless magnets (BEP 60 routing table, KRPC) — magnet support ends at tracker-bearing magnets; decided with the user; would be a fresh effort if ever needed.
- Hand-rolled dev tracker beyond minimal announce duty — it is a test fixture, not a product; no UDP-tracker-grade conformance beyond what'd let us test BEP 15 locally.
- Third-party libraries beyond github.com/jackpal/bencode-go (TUI frameworks like bubbletea/tview, alternative bencode libs) — the dashboard stays hand-rolled ANSI and stdlib-only holds everywhere else; bencode-go is the single deliberate exception because exact re-serialization fidelity is the codec library's job.
