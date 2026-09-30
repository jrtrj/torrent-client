package engine

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"time"

	"torrent-client/internal/state"
	"torrent-client/internal/storage"
	"torrent-client/internal/tracker"
	"torrent-client/internal/wire"
)

// stoppedAnnounceTimeout bounds the goodbye announce on every shutdown path.
// The download context is already cancelled by the time it runs, so this uses
// its own deadline: a tracker that is down must not hold the process open
// after Ctrl-C.
const stoppedAnnounceTimeout = 3 * time.Second

// restore adopts the pieces a resume sidecar claims and the content file still
// proves. The sidecar is never trusted by itself: every claimed piece is
// re-hashed from disk, and one whose bytes no longer match is dropped so the
// download fetches it again. This is what stops a modified or truncated output
// file — or a sidecar copied from another run — from resuming into corrupt
// bytes. Only the pieces that pass are marked verified, so the scheduler never
// asks a peer for them again.
func (e *Engine) restore(store *storage.Storage) error {
	st, err := e.resume.Load()
	switch {
	case errors.Is(err, state.ErrNoState):
		return nil
	case err != nil:
		// A sidecar that is unreadable, foreign, or from another schema is not
		// fatal: the run starts fresh and overwrites it.
		e.logf("engine: ignoring unusable resume sidecar: %v", err)
		return nil
	}

	// Hashing the pieces touches the disk and can take a while, so the reads
	// happen here and the adoption below takes the scheduler lock only to
	// publish the result. The upload path reads the held-piece bitfield under
	// that same lock, so it never sees a half-adopted set.
	var adopted []int
	dropped := 0
	for i := 0; i < e.pieceCount; i++ {
		if !wire.BitfieldHas(st.Have, i) {
			continue
		}
		data, err := store.ReadBlock(i, 0, uint32(e.meta.PieceSize(i)))
		if err != nil {
			return fmt.Errorf("engine: re-verify piece %d from disk: %w", i, err)
		}
		sum := sha1.Sum(data)
		if !bytes.Equal(sum[:], e.meta.PieceHash(i)) {
			dropped++
			continue
		}
		adopted = append(adopted, i)
	}

	e.mu.Lock()
	for _, i := range adopted {
		e.pieces[i].verified = true
		wire.BitfieldSet(e.have, i)
		e.haveCount++
		e.bytesDone += e.meta.PieceSize(i)
	}
	e.mu.Unlock()

	e.logf("engine: resume: %d piece(s) re-verified on disk, %d dropped and queued for download", len(adopted), dropped)
	return nil
}

// persist writes the verified-piece bitfield to the sidecar. It runs after
// each piece verifies, so a SIGKILL that never reaches a clean shutdown still
// resumes with everything verified up to the kill. The bitfield is snapshotted
// under the scheduler lock and the file is written outside it, so a slow disk
// never stalls scheduling.
//
// The write rate is at most one small sidecar write per verified piece: the
// file is a JSON header plus one bit per piece, so for 256 KiB pieces that is
// under a kilobyte of I/O per 256 KiB downloaded, four writes per mebibyte.
// That is negligible next to the piece's own bytes, and it bounds what an
// unclean kill can lose to the pieces that were in flight at that instant.
func (e *Engine) persist() {
	if e.resume == nil {
		return
	}
	e.mu.Lock()
	if !e.stateDirty {
		e.mu.Unlock()
		return
	}
	have := append([]byte(nil), e.have...)
	e.stateDirty = false
	e.mu.Unlock()

	if err := e.resume.Save(have); err != nil {
		// Keep the flag set so the next pass, or the shutdown flush, retries.
		e.mu.Lock()
		e.stateDirty = true
		e.mu.Unlock()
		e.logf("engine: save resume state: %v", err)
	}
}

// finalizeState flushes on the way out in the order that keeps resume safe:
// the content file is synced first and the sidecar that records which pieces
// verified is written last. The sidecar therefore can never name a piece
// whose bytes were not at least handed to the kernel before the record was
// made.
func (e *Engine) finalizeState() {
	if e.resume == nil {
		return
	}
	if e.store != nil {
		if err := e.store.Sync(); err != nil {
			e.logf("engine: sync content file: %v", err)
		}
	}
	e.persist()
}

// announceStopped is the best-effort goodbye on shutdown. It never blocks past
// stoppedAnnounceTimeout, so a dead tracker cannot hang the process.
func (e *Engine) announceStopped() {
	if e.cfg.Tracker == nil {
		return
	}
	e.mu.Lock()
	done := e.bytesDone
	e.lastAnnounce = time.Now()
	e.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), stoppedAnnounceTimeout)
	defer cancel()
	_, err := tracker.AnnounceWithRetry(ctx, e.cfg.Tracker, tracker.AnnounceRequest{
		InfoHash:   e.meta.InfoHash,
		PeerID:     e.cfg.PeerID,
		Port:       e.cfg.Port,
		Uploaded:   e.uploadedBytes(),
		Downloaded: done,
		Left:       e.meta.TotalLength() - done,
		Event:      tracker.EventStopped,
	}, tracker.RetryPolicy{Attempts: 2, BaseDelay: 200 * time.Millisecond, MaxDelay: time.Second})
	if err != nil {
		e.logf("engine: stopped announce failed: %v", err)
	}
}
