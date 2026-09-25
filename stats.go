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
	}
}
