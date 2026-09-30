## Question

Prototype the download dashboard — the single in-place terminal line the engine feeds, plus trial interaction states — as one runnable Thunk/demo script we can react to (this ticket produces the artifact, not the production code; that build belongs to the engine/dashboard implementation session).

1. Take a fake torrent (30 pieces, ~2MB/s current throughput, 4 workers/2 peers) and write the itty-bitty **ANSI-cursor-updating one-liner**: `▕████████░░░░░░░░▏ 42% | 1.8 MB/s | ETA 00:03:12 | 4/5 workers | 12 pieces` — updating in place ~4×/s; snap the animation loop (measure cursor-jump artifacts if the redraw cadence is wrong at 10 lines/s).
2. Extend to `+ events below`: when a peer connects/chokes/dies or a piece verifies, log a *one-line event* under the sticky progress line (prompt-shaped: sticky line + scrolling event lines beneath), using ANSI save/restore-cursor vs erase-line: pick the cleaner mechanism and show why.
3. Handle resize: what the line does when the terminal is 40ch wide; truncate gracefully (no wrap-smear).
4. Bonus polish if trivial: color/redraw threshold (only redraw the line when the integer % changes — avoids terminal strobing at high speed).

Demo script must be self-contained (bash/Go scratch, no deps), runs on our real terminal (input = a fake progress playout), and takes < 200 lines of effort. Put it in `docs/research/dashboard/demo.sh` (or analogous). Its acceptance is: we watch it for 15 seconds and it looks/behaves like the reference screens we want — the prototype IS the art review.

AFTER the demo: the decision is what shape the production `dashboard.go` takes — the interface it exposes to the engine's stats channel (the same shape proposed in the engine ticket), terminal capability detection (dumb terminals → fall back to plain logs), and single-source-of-truth redraw cadence.

## Resolution contract

- the demo script committed and a one-line description of what it demonstrates
- a decision block: production interface shape (what总支 the engine gives it), redraw cadence, resize/truncate policy, event-line mechanism chosen
- whether progress lines go to stdout or stderr (and why that matters with `> out.log`)
