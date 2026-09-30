## Question

Design bandwidth rate limiting (the last bonus feature): a token-bucket limiter template that both the download pump and the upload/serving path consult — keeping the client a good swarm citizen.

Concrete anchor: on a 10-peer download hitting 20 MB/s aggregate, we want to cap at a user-set 2 MB/s without stalling connections (requests stay in flight, just paced).

1. **Token bucket shape**: one global bucket or per-direction (down/up) pair? (pick, with reasoning — torrent clients commonly want `--max-download-rate`/`--max-upload-rate` separately); bucket capacity = burst allowance (e.g. capacity = rate × 0.5s of credit?), refill loop vs lazy-compute-on-take.
2. **Where enforcement lives**: p REQUEST-n loop (`take(n)` blocking), the ack loop — pick the chokepoint: per-block before request-send (limits request pacing) vs per-piece-data before write-disk (limits acceptance) — the two behave differently (one throttles asking, one throttles receiving; the latter is what a bandwidth cap on intake usually means). Say which we do, and what the other buys if we also want it.
3. **API + fairness**: `Limiter` interface (take/try-take with context); per-conn fairness so 1 slow peer can't eat the entire allowance (round-robin vs per-peer sub-allowances — pick simple); what happens to in-flight block requests when the cap drops mid-download (requests already sent can't be un-sent; the backlog absorbs it — prove that with the pipeline-depth numbers).
4. **`Cancel` / fast-shutdown interplay**: limiter holds no locks while a download ctx is cancelled (take must be ctx-aware — the exact Go semantics: select on ctx.Done + token-ready channel).
5. **CLI surface**: flags shape (`-max-down-rate 2MB`) with human-suffix parsing (`512k`, `2M`, none = unlimited) and a stated default (unlimited? some sane cap?) — plus what the dashboard shows when limiting is active.

Depends on: engine ticket's pump/block-queue shape (the take() call sites), seed ticket's serving loop (upload-side call site).

## Resolution contract

- token bucket design (global vs per-direction, capacity/burst math, refill mechanism)
- enforcement chokepoint decision with reasoning (pacing vs acceptance)
- the `Limiter` Go interface + ctx-aware take semantics
- CLI flags + suffix parser rules + dashboard interaction
