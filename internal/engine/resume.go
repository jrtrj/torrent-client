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

// stoppedAnnounceTimeout bounds the shutdown goodbye. The download context is
// already cancelled by then, so the announce carries its own deadline — a dead
// tracker must not hold the process open after Ctrl-C.
const stoppedAnnounceTimeout = 3 * time.Second

// restore adopts the pieces the resume sidecar claims and the content file
// still proves. The sidecar is never trusted on its own: each claimed piece is
// re-hashed from disk and dropped if the bytes changed, which keeps a tampered
// or truncated output file — or a sidecar copied from another run — from
// resuming into corrupt bytes. Only pieces that pass are marked verified.
func (e *Engine) restore(store *storage.Storage) error {
	st, err := e.resume.Load()
	switch {
	case errors.Is(err, state.ErrNoState):
		return nil
	case err != nil:
		// Unreadable, foreign, or from another schema isn't fatal: start fresh
		// and overwrite it.
		e.logf("engine: ignoring unusable resume sidecar: %v", err)
		return nil
	}

	// Re-hashing touches the disk and can take a while, so the reads happen out
	// here and the lock is taken only to publish the result. The upload path
	// reads the bitfield under that same lock, so a half-adopted set never shows.
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
// under the scheduler lock and written outside it, so a slow disk never stalls
// scheduling. The file is a JSON header plus one bit per piece, so 256 KiB
// pieces cost under a kilobyte of I/O each — four writes per mebibyte, nothing
// beside the piece's own bytes, and a cap on what an unclean kill can lose.
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

// finalizeState flushes on the way out in the order resume needs: sync the
// content file first, write the piece record last. The sidecar can then never
// name a piece whose bytes weren't handed to the kernel before the record.
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

// announceStopped is the best-effort goodbye. It never blocks past
// stoppedAnnounceTimeout, so a dead tracker can't hang the process.
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
