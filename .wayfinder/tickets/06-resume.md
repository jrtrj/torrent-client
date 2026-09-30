## Question

Design resumable downloads: interruption at any point — including mid-piece, mid-block, unclean kill — must resume with no re-downloading of verified pieces and no corruption ever visible on the file.

By concrete behavior: a 200MB torrent downloaded 40% then killed mid-piece MUST, on restart, accept only pieces whose SHA-1 matched at write time and stitch the stream without a second download of those bytes. Decide:

1. **State layout**: one `<output>.resume</file>` JSON sidecar (atomic write: temp file + fsync + rename) holding our peer id (stability matters: trackers treat us as the same peer), bitfield-as-hex, per-piece completion, and — the decision — whether the sidecar keys to the torrent by info-hash and output path (multi-torrent safety) and what schema version we stamp.
2. **Partial blocks**: when does a half-downloaded piece's block progress get saved (every block? on shutdown?) — or do we accept piece-granularity resume (simpler, re-fetches the one inflight piece)? The engine's per-conn block queues already track in-flight state; weigh salvaging per-block progress (crash-safe → bigger sidecar, resume-complete guaranteed) against piece-granularity (compact sidecar, refetch ≤1 piece).
3. **Resume flow**: file-exists→resume checks; partial-but-verified piece tail; reconstruction when the torrent is the same but the file on disk was modified (verify-then-resume vs blind-trust sidecar); resume + `have` broadcast so seeders don't re-send us data.
4. **Shutdown choreography**: where the signal handler hooks (engine context), flush ordering (sidecar last), and what happens to announce state on kill (can we send `stopped` on SIGINT with a deadline?).

## Resolution contract

- sidecar schema (full JSON shape + schema versioning policy) and the keying rule
- the partial-block policy decision with its tradeoff explicit
- resume flow step-by-step, including the modified-file case and our re-`have` behavior
- shutdown choreography (who flushes, when, signal handling shape)
