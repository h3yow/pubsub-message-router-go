package router

import (
	"fmt"
	"time"
)

// GapAction decides what happens to a partition key's stream after a gap is
// declared unrecoverable (capacity reached or wait timed out).
type GapAction int

const (
	// GapActionSkip declares the missing seqs lost, jumps the frontier past
	// the gap and releases held messages in order; the stream continues.
	GapActionSkip GapAction = iota
	// GapActionTerminate declares the missing seqs lost and terminates the
	// stream: held messages are dropped and every later publish on the key
	// is rejected with stream_terminated.
	GapActionTerminate
)

func (a GapAction) String() string {
	switch a {
	case GapActionSkip:
		return "skip"
	case GapActionTerminate:
		return "terminate"
	default:
		return "unknown"
	}
}

// OutOfOrderConfig enables the bounded out-of-order recovery window for the
// partition keys selected by Match. Keys not matched keep the legacy
// behavior (seq_gap rejected immediately) unchanged.
type OutOfOrderConfig struct {
	// Match reports whether the window is enabled for (topic, key).
	// nil disables the feature for every key.
	Match func(topic, key string) bool
	// Capacity bounds how many messages may be held per key. Must be > 0.
	Capacity int
	// MaxWait bounds how long a gap may stay open before it is settled.
	// Must be > 0.
	MaxWait time.Duration
	// OnSettle selects the post-settlement behavior; default GapActionSkip.
	OnSettle GapAction
	// SweepInterval is how often expired gaps are settled.
	// <= 0 picks a default derived from MaxWait.
	SweepInterval time.Duration
}

// validate checks the config and derives the sweep interval.
func (c *OutOfOrderConfig) validate() error {
	if c.Capacity <= 0 {
		return fmt.Errorf("out-of-order window: Capacity must be > 0")
	}
	if c.MaxWait <= 0 {
		return fmt.Errorf("out-of-order window: MaxWait must be > 0")
	}
	if c.OnSettle != GapActionSkip && c.OnSettle != GapActionTerminate {
		return fmt.Errorf("out-of-order window: unknown OnSettle action %d", c.OnSettle)
	}
	return nil
}

func (c *OutOfOrderConfig) sweepInterval() time.Duration {
	if c.SweepInterval > 0 {
		return c.SweepInterval
	}
	iv := c.MaxWait / 4
	if iv < 10*time.Millisecond {
		iv = 10 * time.Millisecond
	}
	return iv
}

// gapCause distinguishes why a gap was settled; appears in logs, the journal
// and dedicated counters.
type gapCause string

const (
	gapCauseCapacity gapCause = "capacity"
	gapCauseTimeout  gapCause = "timeout"
)

// pendingEntry is one held message. since is the gap-open timestamp of its
// gap episode (persisted so the wait budget survives restarts).
type pendingEntry struct {
	msg   *Message
	since time.Time
}

// resolveOOO returns the effective settings for a key, or nil when the
// out-of-order window is disabled for it.
func (r *Router) resolveOOO(k topicKey) *OutOfOrderConfig {
	cfg := r.ooo
	if cfg == nil || cfg.Match == nil || !cfg.Match(k.topic, k.key) {
		return nil
	}
	return cfg
}

// pendLocked holds an out-of-order message (msg.Seq > st.nextSeq, window
// enabled, caller holds the shard lock). When the hold would exceed Capacity
// the current gap is settled first and the message re-evaluated, so the
// window never grows past its bound. Released messages (settlement drains
// and in-order drains after an accept) are returned for fan-out after the
// caller drops the lock.
func (r *Router) pendLocked(st *keyState, k topicKey, msg *Message) (PublishResult, []*Message, error) {
	var released []*Message
	for {
		switch {
		case st.terminated:
			return r.reject(msg, RejectStreamTerminated,
				fmt.Sprintf("topic=%s key=%s seq=%d id=%s: stream terminated by gap settlement; not delivered",
					msg.Topic, msg.Key, msg.Seq, msg.ID), st), released, nil
		case msg.Seq < st.nextSeq:
			return r.reject(msg, RejectRollback,
				fmt.Sprintf("topic=%s key=%s seq=%d id=%s: seq behind expected=%d after gap settlement; rejected",
					msg.Topic, msg.Key, msg.Seq, msg.ID, st.nextSeq), st), released, nil
		case msg.Seq == st.nextSeq:
			res, rel, err := r.acceptLocked(st, k, msg)
			return res, append(released, rel...), err
		case len(st.pending) < st.ooo.Capacity:
			// Room to hold: park the message until the gap resolves.
			if st.pending == nil {
				st.pending = make(map[uint64]*pendingEntry)
				st.pendingIDs = make(map[string]uint64)
			}
			if pe, ok := st.pending[msg.Seq]; ok {
				// Same position already held under a different id: explicit
				// conflict, never silently displace the held message.
				return r.reject(msg, RejectDuplicate,
					fmt.Sprintf("topic=%s key=%s seq=%d id=%s: position already held with id=%s; not displaced",
						msg.Topic, msg.Key, msg.Seq, msg.ID, pe.msg.ID), st), released, nil
			}
			if len(st.pending) == 0 {
				st.gapSince = time.Now()
			}
			if r.journal != nil {
				if err := r.journal.AppendPending(msg, st.gapSince.UnixNano()); err != nil {
					return PublishResult{}, released, err
				}
			}
			st.pending[msg.Seq] = &pendingEntry{msg: msg, since: st.gapSince}
			st.pendingIDs[msg.ID] = msg.Seq
			r.counters.gapPending.Add(1)
			r.counters.pendingNow.Add(1)
			r.logf("PENDING topic=%s key=%s seq=%d id=%s basis=(out-of-order window: expected=%d, held=%d/%d, wait_budget=%s)",
				msg.Topic, k.key, msg.Seq, msg.ID, st.nextSeq, len(st.pending), st.ooo.Capacity, st.ooo.MaxWait)
			return PublishResult{
				Pending: true, Reason: RejectGapPending,
				Detail: fmt.Sprintf("topic=%s key=%s seq=%d id=%s: held, expected=%d, held=%d/%d",
					msg.Topic, k.key, msg.Seq, msg.ID, st.nextSeq, len(st.pending), st.ooo.Capacity),
				Message: msg,
			}, released, nil
		default:
			// Holding one more would exceed Capacity: settle the gap now.
			rel, err := r.settleGapLocked(st, k, gapCauseCapacity, msg.Seq)
			released = append(released, rel...)
			if err != nil {
				return PublishResult{}, released, err
			}
		}
	}
}

// acceptLocked commits an in-order message (journal, advance, retain) and
// then drains any held messages the accept made reachable. Caller holds the
// shard lock; released messages are fanned out after the lock is dropped.
func (r *Router) acceptLocked(st *keyState, k topicKey, msg *Message) (PublishResult, []*Message, error) {
	if r.journal != nil {
		if err := r.journal.AppendMessage(msg); err != nil {
			return PublishResult{}, nil, err
		}
	}
	st.advance(msg)
	r.retainLocked(st, msg)
	released, err := r.drainPendingLocked(st, k)
	if err != nil {
		// The message itself is committed; only the drain stopped early.
		// Unreleased entries stay held and are retried by the sweeper or
		// settled on timeout — logged, never silently dropped.
		r.logf("JOURNAL_ERROR topic=%s key=%s during pending drain: %v", k.topic, k.key, err)
	}
	return PublishResult{Accepted: true, Message: msg}, released, nil
}

// drainPendingLocked releases held messages whose seq is now exactly the
// frontier, in order, journaling each before it becomes visible. Caller
// holds the shard lock.
func (r *Router) drainPendingLocked(st *keyState, k topicKey) ([]*Message, error) {
	var released []*Message
	for {
		pe, ok := st.pending[st.nextSeq]
		if !ok {
			break
		}
		if r.journal != nil {
			if err := r.journal.AppendMessage(pe.msg); err != nil {
				return released, err
			}
		}
		st.advance(pe.msg)
		r.retainLocked(st, pe.msg)
		delete(st.pending, pe.msg.Seq)
		delete(st.pendingIDs, pe.msg.ID)
		r.counters.pendingNow.Add(-1)
		r.counters.pendingReleased.Add(1)
		r.logf("RELEASED topic=%s key=%s seq=%d id=%s basis=(gap resolved, held message released in order)",
			k.topic, k.key, pe.msg.Seq, pe.msg.ID)
		released = append(released, pe.msg)
	}
	if len(st.pending) == 0 {
		st.gapSince = time.Time{}
	}
	return released, nil
}

// settleGapLocked declares the current gap unrecoverable. arrival is the seq
// of the message that triggered a capacity settlement (0 for timeouts); the
// arriving message itself is never ruled lost. Lost seqs are
// [nextSeq, lostUpTo]; the verdict is journaled before state changes so a
// restart replays the exact same decision. Caller holds the shard lock.
func (r *Router) settleGapLocked(st *keyState, k topicKey, cause gapCause, arrival uint64) ([]*Message, error) {
	if len(st.pending) == 0 {
		st.gapSince = time.Time{}
		return nil, nil
	}
	limit := minPendingSeq(st)
	if cause == gapCauseCapacity && arrival != 0 && arrival < limit {
		limit = arrival
	}
	lostUpTo := limit - 1
	lost := lostUpTo - st.nextSeq + 1

	if r.journal != nil {
		if err := r.journal.AppendGapSettlement(k.topic, k.key, lostUpTo, string(cause), st.ooo.OnSettle.String()); err != nil {
			return nil, err
		}
	}
	switch cause {
	case gapCauseCapacity:
		r.counters.gapSettledCapacity.Add(1)
	case gapCauseTimeout:
		r.counters.gapSettledTimeout.Add(1)
	}
	r.counters.seqsDeclaredLost.Add(int64(lost))
	r.logf("GAP_SETTLED topic=%s key=%s lost_seqs=[%d..%d] cause=%s action=%s held=%d",
		k.topic, k.key, st.nextSeq, lostUpTo, cause, st.ooo.OnSettle, len(st.pending))

	if st.ooo.OnSettle == GapActionTerminate {
		st.terminated = true
		r.counters.terminatedKeys.Add(1)
		// Drop every held message explicitly: counted and logged per message.
		for seq, pe := range st.pending {
			r.counters.terminatedPendDrops.Add(1)
			r.counters.pendingNow.Add(-1)
			r.logf("PENDING_DROPPED topic=%s key=%s seq=%d id=%s basis=(stream terminated, held message dropped)",
				k.topic, k.key, pe.msg.Seq, pe.msg.ID)
			delete(st.pending, seq)
			delete(st.pendingIDs, pe.msg.ID)
		}
		st.gapSince = time.Time{}
		return nil, nil
	}

	// GapActionSkip: jump the frontier past the lost range and release
	// whatever is now reachable, in order.
	st.nextSeq = lostUpTo + 1
	released, err := r.drainPendingLocked(st, k)
	if err != nil {
		r.logf("JOURNAL_ERROR topic=%s key=%s during settlement drain: %v", k.topic, k.key, err)
	}
	if len(st.pending) > 0 {
		// A new gap episode starts with a fresh wait budget.
		st.gapSince = time.Now()
	}
	return released, nil
}

func minPendingSeq(st *keyState) uint64 {
	var min uint64
	for seq := range st.pending {
		if min == 0 || seq < min {
			min = seq
		}
	}
	return min
}

// sweepExpired settles every gap whose wait budget ran out. Released
// messages are fanned out after each shard lock is dropped, so a slow
// subscriber can never block the sweeper or any publisher.
func (r *Router) sweepExpired() {
	now := time.Now()
	for _, sh := range r.shards {
		sh.mu.Lock()
		var released []*Message
		for k, st := range sh.state {
			if st.ooo == nil || st.terminated || len(st.pending) == 0 {
				continue
			}
			if now.Sub(st.gapSince) < st.ooo.MaxWait {
				continue
			}
			rel, err := r.settleGapLocked(st, k, gapCauseTimeout, 0)
			if err != nil {
				r.logf("JOURNAL_ERROR topic=%s key=%s during timeout settlement: %v", k.topic, k.key, err)
				continue
			}
			released = append(released, rel...)
		}
		sh.mu.Unlock()
		for _, m := range released {
			r.fanout(m)
		}
	}
}

// sweepLoop settles expired gaps until the router is closed.
func (r *Router) sweepLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-t.C:
			r.sweepExpired()
		}
	}
}
