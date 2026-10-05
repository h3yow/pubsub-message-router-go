package router

import "sync/atomic"

// Counters holds router-wide observable counters, updated atomically.
// Every non-routine disposition (duplicate, rollback, gap, back-pressure
// drop) has its own counter so overflow/error behavior is observable and
// distinguishable from normal acceptance.
type Counters struct {
	published           atomic.Int64
	accepted            atomic.Int64
	rejected            atomic.Int64
	duplicates          atomic.Int64
	seqRollbacks        atomic.Int64
	seqGaps             atomic.Int64
	delivered           atomic.Int64
	droppedBackpressure atomic.Int64
	subscribersLive     atomic.Int64
	replayed            atomic.Int64

	// Out-of-order window dispositions.
	gapPending          atomic.Int64 // total messages held by the window
	pendingNow          atomic.Int64 // currently held (gauge)
	pendingReleased     atomic.Int64 // held messages later released in order
	gapSettledCapacity  atomic.Int64 // gaps settled because capacity ran out
	gapSettledTimeout   atomic.Int64 // gaps settled because the wait budget ran out
	seqsDeclaredLost    atomic.Int64 // seq numbers ruled unrecoverable
	streamTerminated    atomic.Int64 // publishes rejected on a terminated stream
	terminatedKeys      atomic.Int64 // keys whose stream was terminated
	terminatedPendDrops atomic.Int64 // held messages dropped on termination
}

// Stats is a point-in-time immutable snapshot of Counters.
type Stats struct {
	Published           int64 `json:"published"`
	Accepted            int64 `json:"accepted"`
	Rejected            int64 `json:"rejected"`
	Duplicates          int64 `json:"duplicates"`
	SeqRollbacks        int64 `json:"seq_rollbacks"`
	SeqGaps             int64 `json:"seq_gaps"`
	Delivered           int64 `json:"delivered"`
	DroppedBackpressure int64 `json:"dropped_backpressure"`
	SubscribersLive     int64 `json:"subscribers_live"`
	Replayed            int64 `json:"replayed"`

	GapPending             int64 `json:"gap_pending"`
	PendingNow             int64 `json:"pending_now"`
	PendingReleased        int64 `json:"pending_released"`
	GapSettledCapacity     int64 `json:"gap_settled_capacity"`
	GapSettledTimeout      int64 `json:"gap_settled_timeout"`
	SeqsDeclaredLost       int64 `json:"seqs_declared_lost"`
	StreamTerminated       int64 `json:"stream_terminated"`
	TerminatedKeys         int64 `json:"terminated_keys"`
	TerminatedPendingDrops int64 `json:"terminated_pending_drops"`
}

func (c *Counters) snapshot() Stats {
	return Stats{
		Published:           c.published.Load(),
		Accepted:            c.accepted.Load(),
		Rejected:            c.rejected.Load(),
		Duplicates:          c.duplicates.Load(),
		SeqRollbacks:        c.seqRollbacks.Load(),
		SeqGaps:             c.seqGaps.Load(),
		Delivered:           c.delivered.Load(),
		DroppedBackpressure: c.droppedBackpressure.Load(),
		SubscribersLive:     c.subscribersLive.Load(),
		Replayed:            c.replayed.Load(),

		GapPending:             c.gapPending.Load(),
		PendingNow:             c.pendingNow.Load(),
		PendingReleased:        c.pendingReleased.Load(),
		GapSettledCapacity:     c.gapSettledCapacity.Load(),
		GapSettledTimeout:      c.gapSettledTimeout.Load(),
		SeqsDeclaredLost:       c.seqsDeclaredLost.Load(),
		StreamTerminated:       c.streamTerminated.Load(),
		TerminatedKeys:         c.terminatedKeys.Load(),
		TerminatedPendingDrops: c.terminatedPendDrops.Load(),
	}
}
