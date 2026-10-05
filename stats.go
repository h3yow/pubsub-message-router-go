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

	// Bounded reorder dispositions.
	heldNow          atomic.Int64
	heldTotal        atomic.Int64
	releasedFromHold atomic.Int64
	gapsFilled       atomic.Int64
	gapsLostCapacity atomic.Int64
	gapsLostTimeout  atomic.Int64
	streamsEnded     atomic.Int64
	missingSeqs      atomic.Int64
	streamRejects    atomic.Int64
	heldConflicts    atomic.Int64
	gapLostRejects   atomic.Int64
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

	HeldNow          int64 `json:"reorder_held_now"`
	HeldTotal        int64 `json:"reorder_held_total"`
	ReleasedFromHold int64 `json:"reorder_released"`
	GapsFilled       int64 `json:"reorder_gaps_filled"`
	GapsLostCapacity int64 `json:"reorder_gaps_lost_capacity"`
	GapsLostTimeout  int64 `json:"reorder_gaps_lost_timeout"`
	StreamsEnded     int64 `json:"reorder_streams_ended"`
	MissingSeqs      int64 `json:"reorder_missing_seqs"`
	StreamRejects    int64 `json:"reorder_stream_rejects"`
	HeldConflicts    int64 `json:"reorder_held_conflicts"`
	GapLostRejects   int64 `json:"reorder_gap_lost_rejects"`
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

		HeldNow:          c.heldNow.Load(),
		HeldTotal:        c.heldTotal.Load(),
		ReleasedFromHold: c.releasedFromHold.Load(),
		GapsFilled:       c.gapsFilled.Load(),
		GapsLostCapacity: c.gapsLostCapacity.Load(),
		GapsLostTimeout:  c.gapsLostTimeout.Load(),
		StreamsEnded:     c.streamsEnded.Load(),
		MissingSeqs:      c.missingSeqs.Load(),
		StreamRejects:    c.streamRejects.Load(),
		HeldConflicts:    c.heldConflicts.Load(),
		GapLostRejects:   c.gapLostRejects.Load(),
	}
}
