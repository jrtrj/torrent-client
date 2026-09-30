## Question

Write `docs/build-spec.md` — the destination artifact: consolidate every closed ticket's resolution into the single engineering spec a fresh implementer (or an implementation-session agent) can build from. This ticket runs LAST, after all others close.

Structure (proposed, adjustable during the session):

1. Overview + design principles (our own code, credits to the task brief; bencode-go as the sole dependency with the round-trip caveat handled how the research decided)
2. Architecture (per-subsystem sections mirroring the map: metainfo/info-hash, bencode codec usage, tracker layer + interface, wire protocol contract, piece engine + concurrency, dashboard, resume/state, seeding, then the four bonus features in sequence)
3. CLI contract (command shapes, flags, exit codes)
4. Verification plan (the local swarm: dev tracker + seeder harness; the real-torrent test; per-feature test matrices — each subsystem's tests listed)
5. Implementation sequencing for build sessions (the order, what each session delivers, what 'done' means per layer) — limited to what's decided; open design notes stay in their tickets.

Rules: every claim traces to a ticket's resolution comment (link inline like `- per wire contract (see ticket #N): ...`); no new decisions get made in the spec — anything discovered unresolved during writing gets a flag either for a new targeted ticket (if blocking) or an explicit documented default (if peripheral). The spec is written by re-reading each closed ticket's resolution comment — not from memory — and is valid only when its last line cites all ticket numbers.

## Resolution contract

- docs/build-spec.md committed, links + cites all tickets
- a map Notes update: 'destination artifact exists'
- fog section cleared: anything not ticketed during the writing of the spec either got its ticket, a documented default, or was ruled out of scope
