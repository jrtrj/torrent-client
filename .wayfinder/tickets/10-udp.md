## Question

Design UDP tracker support (BEP 15) as the second transport behind the tracker interface from the earlier tracker ticket — plug-in, no engine changes.

1. **Protocol flow**: the 3-step dance (CONNECT: 64-bit connection_id from the 8-byte magic `0x41727101980`; ANNOUNCE; SCRAPE if trivial) — exact packet layouts (action ids, transaction ids, byte offsets), because every field is big-endian fixed-offset binary, no bencoding.
2. **Transaction id + connection id lifecycle**: connection_id validity ~1 minute per spec — cache/reuse within that window or re-CONNECT per announce? transaction id generation + response matching (and the spoofed-response hazard: match on our own ids); retry on timeout (the spec's exponential backoff start 15s ×2 to 384s, 8 attempts) — adopt/adjust for our client (that's ~8 min worst case; decide our cap).
3. **Announce mapping**: the UDP announce's fixed-width 98-byte packet — how HTTP announce params (numwant, key, event) map into it; IPv4 peers from the response (`peers` in 6-byte chunks, same binary layout as compact HTTP) — no IPv6 in BEP 15 (BEP 7/`peers6`-style is separate; state the limitation).
4. **Error/warning handling**: the response's action=3 error frames (text message); when the tracker returns warnings-as-errors; malformed/short datagrams → drop or retry?
5. **Socket shape**: one UDP socket per tracker? per process? (source-port consistency matters to trackers); timeouts per packet, not per session; and how scheme selection works in the tracker interface (`udp://` announce URL → UDP transport, `http(s)://` → HTTP, one dispatcher).

Depends on: BEP 15 research for the spec facts (packet hex-layouts, magic number, backoff numbers); tracker interface ticket's `Tracker` interface.

## Resolution contract

- packet layouts (connect/announce/error) as byte-exact tables + the magic constant
- connection-id lifecycle decision + retry/backoff policy (with our cap)
- announce parameter mapping table + response decode rules
- the transport switch design (how announce URLs select HTTP vs UDP) — the go signature on the tracker interface stays untouched
