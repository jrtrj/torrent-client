## Question

Design the tracker layer end-to-end: one interface, two transports (HTTP now, UDP as a later ticket plugs in).

1. **Announce contract**: every parameter (`info_hash`, `peer_id`, `port`, `uploaded`, `downloaded`, `left`, `compact=1`, `numwant`, `event`) — required vs optional, URL-encoding of raw binary info-hash/peer_id (the `+`/`%2B` trap: a peer id whose first 6 bytes are all '+' must round-trip), and the event lifecycle (empty→started→periodic→stopped/completed) and when each fires (including the point where `left=0` flips us to seeder).
2. **Interval handling**: respect the tracker's `interval` (and `min_interval` if present); schedule re-announces in-process so a long-running seeder stays discoverable.
3. **Response parsing**: `complete`/`incomplete`/`peers`/`peers6` compact form; the id-prefixed peer key trap (`t<id-prefix>peers` with id prefix = first 6 bytes of our peer id ×2) — parse via bencode-go's Dict fallback when the fixed struct misses; binary peers blob = 6 bytes/peer (4 IP big-endian + 2 port big-endian); parse `peers6` for IPv6 too or punt with a stated limitation.
4. **Failure policy**: tracker down/unreachable/rate-limited → retry with backoff, keep swarm state, never kill an in-flight download; distinguish fatal (bad info-hash echo) from transient (5xx, timeout).
5. **Port/dial posture**: what port we announce (listener where we accept inbound peers), zero-conf defaults, and private/loopback swarm behavior — the dev tracker case (we announce to 127.0.0.1:9xxx; loopback must not be filtered).
6. **Interface shape**: `Tracker` interface — method names, inputs (info-hash, peer id, our port, stats snapshot), output peer-set + interval; so HTTP and UDP both satisfy it, and the engine owns scheduling, not the transports.

Feeds: tracker implementation, dev tracker expectations, seeding (announces `completed`), and the engine (peer-set churn feeding the dialer).

## Resolution contract

- the tracker interface (full Go signature set) + announce parameter table with encode rules and event lifecycle
- response schema incl. compact peers decoding rules + the id-prefixed-peers key trap handled by explicit Dict fallback
- failure/retry policy table + interval scheduling ownership (engine-side)
- draft http_tracker.go API surface
