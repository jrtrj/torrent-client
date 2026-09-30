## Question

Design magnet link support (BEP 9/ut_metadata) for tracker-bearing magnets — the first bonus feature, and the one the user wants sequenced first.

Entry: user pastes `magnet:?xt=urn:btih:<40-hex-or-32-base32>&tr=<tracker-url>*` → client resolves metadata from swarms peers and downloads. DHT is out of scope (decided) — magnets must carry at least one `tr=` tracker; zero-tracker magnets error out with a clear message.

1. **Magnet parsing**: URI grammar (`xt`, `tr` (multi), `dn`, `xl`, acceptable-and-ignored keys); info-hash forms — 40-char hex AND 32-char base32 (both are seen in the wild; must accept both, normalize to [20]byte).
2. **Metadata acquisition via ut_metadata (BEP 9)**: the extension handshake (`USEXT` message, handshake dict: `m.ut_metadata = 1`, metadata_size, `v` string) — frame shape, our extension-message IDs (local negotiation), the ut_metadata request/data/reject messages (bencoded dicts with `msg_type`/`piece`/`total_size`, binary metadata payload NOT inside the bencode — the classic framing trap).
3. **Metadata assembly + verification**: reassemble `info` dict pieces; SHA-1 the reassembled bytes == info-hash from the magnet (the trust anchor!) — vs a malicious peer sending bogus bytes; parallel requests to multiple peers with early-exit on verified success; timeout/fallback budget.
4. **Integration**: after metadata resolves, the magnet flow converges into the normal torrent struct → tracker announce → engine; what the CLI surface looks like (`torrent-client magnet "<uri>" out/`), and what we do when the tracker returns no peers (retry/backoff? error?).
5. **Peer capability discovery**: peers advertising ut_metadata only via extension handshake — how we filter/choose metadata-capable peers from the swarm, and behavior when nobody supports it (timeout with clear error).

Depends on: BEP 9 research (spec facts), wire protocol contract (for the extension message frame), tracker design (trackers feed the swarm).

## Resolution contract

- magnet grammar design + both info-hash encodings handled
- the ut_metadata message flow (frames, msg ids, verification policy) — enough that an implementer writes it without reading BEP 9
- integration call flow (magnet → metadata → torrent struct → normal pipeline) + CLI shape
- failure matrix (no peers, peer lies, partial pieces timeout)
