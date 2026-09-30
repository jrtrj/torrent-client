## Question

Leaving the seeder/serving side designed: our client must upload completed pieces back to the swarm (otherwise: leech-only client, and evaluation weights this feature). Scope our answer to what Serving means for us — one listener, one upload path — not a general-purpose server.

1. **Listener lifecycle**: bound to our announced port (same one the tracker knows); the accept loop; per-conn handshake validation (info-hash must match a torrent we hold — decide: any of our torrents, or a per-torrent allow-list); what happens on a bad handshake (drop silently? log rate?).
2. **Concurrent inbound conns** and their pump loops: one goroutine per serving conn (same pump model as the download side); a serving conn reads requests and writes pieces; a serving conn NEVER resolves conflicts between its own send queue and the peer's cancel.
3. **Request handling**: per request (piece index/begin/length from the wire): bounds check (request must be in-ours-and-complete, and length≤128KB), file I/O pattern (seek+read from disk at request time, or in-memory piece cache — pick, with the memory bound for a multi-GB torrent), then enqueue a piece message; flow control: max un-served request backlog (tune for 16KB blocks), then choke policy for inbound peers (we send `choke`/`unchoke`; what policy? always choke except while serving? round-robin upload slots?) — pick a simple, state-limited policy and say why.
4. **Sharing posture**: announce `uploaded` counters to trackers; do we prefer uploading to peers who reciprocate (a tit-for-tat tinge) or is any-asker-gets-served the right simple answer for a fresh client? (Pick one; record the other as a stretch.)
5. **Serving while downloading**: the same process must serve and download concurrently; what shared state (engine bitfields, disk cache) they touch and how concurrent reads are kept safe.

## Resolution contract

- the accept/handshake/pump/serving design with real types
- upload slot + choke policy for incoming peers (with the "why")
- bounds check list for requests (with limits)
- file I/O / caching decision + concurrency guarantees with the downloading side
