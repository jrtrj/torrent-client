package engine

import (
	"sort"

	"torrent-client/internal/metainfo"
	"torrent-client/internal/wire"
)

// PeerStat is one peer's state in a Stats snapshot.
type PeerStat struct {
	Addr string
	// PeerID is the id the peer sent in its handshake.
	PeerID [20]byte
	// Choked and Interested are the peer's own state towards us.
	Choked     bool
	Interested bool
	// Pieces is how many pieces the peer advertises.
	Pieces int
	// Outstanding is how many block requests are in the pipe to this peer.
	Outstanding int
	// Served is how many blocks this peer has delivered this session.
	Served int
	// Stalled is how many of its requests expired unanswered.
	Stalled int
	// State is "connected" or "blacklisted".
	State string
}

// Stats is a point-in-time snapshot of a download. It is plain data with no
// engine handles in it, so the dashboard, resume, and seeding tickets can
// consume it without reaching into the engine.
type Stats struct {
	Pieces     int
	PiecesDone int
	BytesTotal int64
	// BytesDone counts verified bytes only.
	BytesDone int64
	// BytesIn counts every block byte accepted, including data later thrown
	// away by a failed hash check.
	BytesIn int64

	BlocksRequested int64
	BlocksReceived  int64
	// BlocksDuplicate counts blocks that arrived for bytes already held;
	// stalled blocks were re-issued and answered twice.
	BlocksDuplicate int64
	// BlocksStalled counts requests that expired unanswered.
	BlocksStalled int64
	BadPieces     int64
	PeersDropped  int64
	// PeersUsed counts connected peers that delivered at least one block.
	PeersUsed int
	// PeersActive counts peers with work in the request pipeline right now;
	// PeersMaxActive is the session peak of that number.
	PeersActive    int
	PeersMaxActive int

	// Blacklisted lists peer addresses dropped for the session.
	Blacklisted []string
	Peers       []PeerStat
	// Have is a copy of the bitfield of verified pieces.
	Have []byte
}

// Meta is the torrent this engine is downloading. It is fixed for the engine's
// life, so the upload path can read piece sizes and hashes from it.
func (e *Engine) Meta() *metainfo.MetaInfo { return e.meta }

// Have reports whether piece index has been downloaded, hash-verified, and
// written, and so can be uploaded. It takes the scheduler's lock, so it is
// safe to call from the upload path while Run is going; it returns false for
// every piece until the download verifies them.
func (e *Engine) Have(index int) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return wire.BitfieldHas(e.have, index)
}

// HaveBitfield returns a copy of the verified-piece bitfield, so the upload
// path can advertise everything it holds in one snapshot. Safe to call while
// Run is going.
func (e *Engine) HaveBitfield() []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]byte(nil), e.have...)
}

// Stats returns a snapshot of the download. Safe to call while Run is going.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()

	s := Stats{
		Pieces:          e.pieceCount,
		PiecesDone:      e.haveCount,
		BytesTotal:      e.meta.TotalLength(),
		BytesDone:       e.bytesDone,
		BytesIn:         e.counters.bytesIn,
		BlocksRequested: e.counters.blocksRequested,
		BlocksReceived:  e.counters.blocksReceived,
		BlocksDuplicate: e.counters.blocksDuplicate,
		BlocksStalled:   e.counters.blocksStalled,
		BadPieces:       e.counters.badPieces,
		PeersDropped:    e.counters.peersDropped,
		PeersMaxActive:  e.counters.peersMaxActive,
	}
	if e.have != nil {
		s.Have = append([]byte(nil), e.have...)
	}
	for addr, p := range e.peers {
		if p.window() > 0 {
			s.PeersActive++
		}
		if p.served > 0 {
			s.PeersUsed++
		}
		available := 0
		for i := 0; i < e.pieceCount; i++ {
			if wire.BitfieldHas(p.bits, i) {
				available++
			}
		}
		s.Peers = append(s.Peers, PeerStat{
			Addr:        addr,
			PeerID:      p.peerID,
			Choked:      p.choked,
			Interested:  p.interested,
			Pieces:      available,
			Outstanding: p.window(),
			Served:      p.served,
			Stalled:     p.stalls,
			State:       "connected",
		})
	}
	for addr := range e.blacklist {
		s.Blacklisted = append(s.Blacklisted, addr)
	}
	sort.Strings(s.Blacklisted)
	sort.Slice(s.Peers, func(i, j int) bool { return s.Peers[i].Addr < s.Peers[j].Addr })
	return s
}
