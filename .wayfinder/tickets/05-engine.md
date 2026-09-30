## Question

Design the piece engine: the concurrency model that turns a peer set + a backlog of piece works into a verified file. Walk the whole flow with concrete numbers — a 30-piece torrent, 5 workers, a peer that has 10 of the pieces.

1. **Work lifecycle**: single authoritative pending-piece state; retry on peer death/integrity failure; duplicate-work elimination (no two workers on the same piece — reference bug illustration: worker finds piece absent from peer's bitfield, re-enqueues it, and every worker hits it forever at 100% CPU and zero progress).
2. **Max pipeline depth per peer**: 5 to 10 simultaneous 16KB block requests in flight per connection — pick our number and the ramp; how backpressure is enforced (request queue bound with block-level ack); stalled-request expiry (what wall-clock grace before a block is re-requested elsewhere — 30s reference default vs tighter) and the per-peer request ledger.
3. **Half-connection handling**: main loop accepts from many peers; pick-and-stick policy: contact peers we might need (choking us but has wanted pieces) to request unchoke as they become able; decide retain/drop criteria (choked+idle peers) — the engine's long-run conns possession rule.
4. **Message pump placement** (critical): SELECT pumping model — the conn object owns its pump loop (its read side runs free; each worker reads only its own result channel and writes to the shared work/result channels or into the conn's pipeline). The clean model beats the reference build where message handling and per-piece download loops are one interleaved stack per piece. DOES the pump stay read-through buffered inside a per-conn goroutine with a block-response dispatcher in the conn object? Pick the model that matches the wire framing we control — and mind that choke state is conn-owned, not worker-owned.
5. **Integrity + accounting**: SHA-1 verify before accept; bitfield update; `have` broadcast to connected peers; a bad peer is blacklisted for the session (retry elsewhere); piece-boundary math (last piece shorter) — data structures for a torrent of N pieces where piece lengths vary only at the tail.
6. **Progress + observability contract**: what the engine exports to the dashboard/resume/seeder — pieces done, block-level progress, bytes in, peer states, worker liveness (the exact stat/channel shape the other tickets consume). Engine holds no UI code.

## Resolution contract

- the engine's concurrency model decision (pump ownership, worker pool shape, backpressure mechanics) with the dangling-connection reasoning
- data structures (pending piece state, per-conn block queues, bitfield, statistics) with field-level detail
- the observability contract (types/channels) consumed by dashboard/resume/seeder tickets
- failure policy table (which fail → requeue, which → drop conn/peer, which → abort)
