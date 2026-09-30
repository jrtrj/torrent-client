## Question

Define the total on-the-wire contract we will implement, as our own renegotiated surface: every decision the reference got wrong is fixed here by spec, not inherited. (The repo's reference files are gone from the worktree — pull them from git history if a before/after comparison helps the walkthrough.)

Cover, per message and mechanism, with CLI-shape analogies where useful:

1. **Handshake**: exact 68-byte frame (strlen=19, "BitTorrent protocol", 8 reserved, infohash, peer id). Decide reserved-bytes values: do we advertise DHT byte, extension protocol — knowing magnets (BEP 9) and UDP trackers are downstream features of this same client?
2. **Bitfield**: hole numbering (MSB=0), empty-bitfield legality, `have` aggregation, and the exact rejection behavior when a peer's bitfield is overlong or references pieces ≥ our count.
3. **Messages**: encode/decode shape for choke/unchoke/interested/not_interested/have/bitfield/request/piece/cancel/keep-alive; length-prefix rules; the request payload field order (what a swapped order does to a swarm); block-size constants (16KB request clamp), and legal bounds checks on incoming piece payloads (begin+len ≤ piece length).
4. **State machine**: choke state as *shared state on the client object, not per-download*; write deadlines vs read deadlines on the same conn; behavior on receiving unrequested or duplicate blocks; cancel semantics.
5. **Error taxonomy**: which protocol misbehaviors are connection-fatal (bad handshake echo, bitfield index overflow) vs recoverable (bad block → re-request from someone else) — and what each maps to in worker behavior.

The endpoint spec is the input to: the piece-engine grilling (doubleruns backtrack to read this), the seeding handler (its message handling mirrors ours), and every verification test in the build spec.

## Resolution contract

- a message-by-message contract table (frame layout, bounds checks, on-violation action) with the exact constants (timeouts, block sizes, pipeline depths as *minimums* to satisfy)
- explicit statements of the renegotiated decisions vs the buggy reference behavior (at minimum: LittleEndian request payloads, bitfield MSB indexing, single-owner choke state)
- the reserved-bytes decision and its consequence for magnet/DHT upstream features
