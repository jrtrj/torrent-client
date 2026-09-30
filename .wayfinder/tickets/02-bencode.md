## Question

Pin down everything we must replicate from jackpal/bencode-go's behavior before adopting it as our one dependency.

By requirement, the tracker response parser MUST tolerate trackers that put `peers` inside a key prefixed by our client identification — `t.tracker_resp.Peers = body` semantics. Bencode-go supports that via its `Dict` fallback (unmarshal the response as a generic dict, then read `peers` from the `t<client-id>` key) — but the exact re-serialization side is stricter:

Specifically answer:

1. byte-for-byte round-trip fidelity: if we read a `.torrent`, extract the raw `info` bytes, and later re-marshall the parsed values, does jackpal's marshaler produce byte-identical output to the original `info` dict (integer formatting, string ordering, unicode handling)? If not, what fallback (raw-bytes capture at decode time, hand-rolled re-encoder only for `info`) preserves the guarantee that `info_hash` matches what trackers and peers verify?
2. how does bencode-go type the inflexible bits — 64-bit sizes (`length` on huge files), the `pieces` string (binary, not UTF-8-safe), multi-value dictionary key ordering — and what struct tags / `interface{}` reads do we need?
3. what comes back from `bencode.Unmarshal` on a malformed file (empty vs error), and does an error path need a salvage attempt via `Dict`?
4. confirm today's v1.0.x release: latest version, go.mod line, license (settle the version pin and record it).

The working context to research against: the tracker's response shape is a bencode dict whose `peers` key may be `"peers"` or `"<id-prefix>peers"` (id prefix = first 6 bytes of the requesting peer id ×2) — dict-fallback parsing must be part of the tracker design. Everything is Go 1.27, stdlib-everywhere-else by standing decision.

## Resolution contract

- round-trip verdict (safe / unsafe) + evidence (a probe test result, on a scratch dir, with byte-level comparison over real .torrent fixtures — including one with unicode filename and one with `piece length` > 2^31/2 boundary values)
- the exact codec API surface we'll adopt (functions + types), incl. the dict-fallback read for tracker responses and the info-hash raw-capture pattern
- pinned version + license note
