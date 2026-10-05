package router

import (
	"fmt"
	"sort"
	"time"
)

// GapBasis is the durable, distinguishable reason a pending gap reached a
// final conclusion.
type GapBasis string

const (
	// GapFilled: every previously missing seq arrived within the bounds; the
	// parked messages were released in original order.
	GapFilled GapBasis = "filled"
	// GapCapacity: the bounded hold buffer reached its capacity and the still
	// missing seqs were declared permanently unfillable.
	GapCapacity GapBasis = "capacity"
	// GapTimeout: the maximum wait for the oldest parked message elapsed and
	// the still missing seqs were declared permanently unfillable.
	GapTimeout GapBasis = "timeout"
)

// GapAction decides the fate of messages parked behind a gap once that gap is
// declared unfillable.
type GapAction int

const (
	// GapContinue: the missing seqs are reported as lost; parked messages
	// above the gap are released in order and the stream continues from the
	// new contiguous frontier.
	GapContinue GapAction = iota
	// GapTerminate: the missing seqs are reported as lost and the whole
	// partition stream is failed closed. Later publishes on the key receive
	// RejectStreamTerminated; order is never resumed on a broken prefix.
	GapTerminate
)

func (a GapAction) String() string {
	switch a {
	case GapContinue:
		return "continue"
	case GapTerminate:
		return "terminate"
	default:
		return "unknown"
	}
}

// ReorderPolicy is the per partition key bounded out-of-order tolerance.
type ReorderPolicy struct {
	// MaxHeld bounds how many out-of-order messages may be parked behind one
	// or more missing seqs. Must be >= 1 when enabled.
	MaxHeld int
	// MaxWait bounds how long (measured from parking the oldest currently
	// parked message) the router waits for missing seqs to arrive. Must be > 0
	// when enabled.
	MaxWait time.Duration
	// OnUnfillable selects what happens to parked messages once the gap is
	// declared permanently unfillable (capacity or wait bound reached).
	OnUnfillable GapAction
}

// ReorderSelector decides per (topic,key) whether bounded reorder buffering is
// enabled. Keys for which it returns false keep the exact historical
// strict-contiguous semantics.
type ReorderSelector func(topic, key string) (ReorderPolicy, bool)

// GapEvent is emitted once per gap conclusion: a successful fill, or an
// unfillable gap settled by capacity/wait bound. Each conclusion is also
// counted separately and logged with topic, seqs and basis.
type GapEvent struct {
	Topic    string
	Key      string
	Basis    GapBasis
	Action   GapAction // action taken when Basis != GapFilled
	Missing  []uint64  // seqs declared permanently unfillable (empty on fill)
	Released []uint64  // seqs released from the hold, in ascending order
	Ended    []uint64  // parked seqs discarded with the stream (terminate only)
	Wait     time.Duration
}

// GapCallback observes gap conclusions. Never call blocking router methods
// (Publish/Subscribe/Close) from it: it runs inside the partition shard lock.
type GapCallback func(GapEvent)

// heldEntry is one out-of-order message parked behind a missing seq.
type heldEntry struct {
	msg    *Message
	heldAt time.Time
}

// reorderPolicyFor resolves (and caches on the state) the policy for a key.
// The selector result is cached so policy lookups stay stable for the lifetime
// of the state and recovery can rely on one consistent answer.
func (r *Router) reorderPolicyFor(st *keyState) (ReorderPolicy, bool) {
	if st.policy != nil {
		return *st.policy, true
	}
	if st.policyChecked {
		return ReorderPolicy{}, false
	}
	st.policyChecked = true
	if r.reorder == nil {
		return ReorderPolicy{}, false
	}
	p, ok := r.reorder(st.tk.topic, st.tk.key)
	if !ok {
		return ReorderPolicy{}, false
	}
	if p.MaxHeld < 1 || p.MaxWait <= 0 {
		// Misconfiguration is a hard error for the key, never a silent
		// downgrade to another semantic.
		panic(fmt.Sprintf("router: invalid reorder policy for topic=%s key=%s: MaxHeld=%d MaxWait=%s",
			st.tk.topic, st.tk.key, p.MaxHeld, p.MaxWait))
	}
	st.policy = &p
	return p, true
}

// park holds one out-of-order message and (re)arms the wait timer. The caller
// holds the shard lock. Hold records are persisted before the message is
// considered held, so a crash cannot forget a parked message.
func (r *Router) parkLocked(st *keyState, p ReorderPolicy, msg *Message, now time.Time) error {
	if st.held == nil {
		st.held = make(map[uint64]*heldEntry)
	}
	if err := r.journal.AppendHold(st.tk.topic, st.tk.key, now, msg); err != nil {
		return err
	}
	st.held[msg.Seq] = &heldEntry{msg: msg, heldAt: now}
	st.recent[msg.ID] = msg.Seq
	st.order = append(st.order, msg.Seq)
	if len(st.order) > dedupWindow {
		victim := st.order[0]
		st.order = st.order[1:]
		for id, seq := range st.recent {
			if seq == victim {
				delete(st.recent, id)
				break
			}
		}
	}
	r.counters.heldNow.Add(1)
	r.counters.heldTotal.Add(1)

	oldest := now
	for _, h := range st.held {
		if h.heldAt.Before(oldest) {
			oldest = h.heldAt
		}
	}
	st.gapDeadline = oldest.Add(p.MaxWait)
	r.armGapTimerLocked(st, p)
	return nil
}

func (r *Router) armGapTimerLocked(st *keyState, p ReorderPolicy) {
	d := time.Until(st.gapDeadline)
	if d < 0 {
		d = 0
	}
	if st.timer != nil {
		st.timer.Stop()
	}
	st.timer = time.AfterFunc(d, func() { r.onGapTimeout(st) })
}

func (r *Router) stopGapTimerLocked(st *keyState) {
	if st.timer != nil {
		st.timer.Stop()
		st.timer = nil
	}
}

// onGapTimeout settles an open gap whose wait bound elapsed.
func (r *Router) onGapTimeout(st *keyState) {
	shard := r.shardFor(st.tk)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	// This callback consumed the timer; clear it so a settlement that empties
	// the hold cannot re-fire itself.
	st.timer = nil
	if st.terminated || len(st.held) == 0 {
		return
	}
	p, ok := r.reorderPolicyFor(st)
	if !ok {
		return
	}
	oldest := st.held[minHeldSeq(st.held)].heldAt
	if time.Now().Before(oldest.Add(p.MaxWait)) {
		// Re-armed for a newer (later) deadline before this callback ran.
		r.armGapTimerLocked(st, p)
		return
	}
	msgs, err := r.settleUnfillableLocked(st, p, GapTimeout, time.Since(oldest))
	if err != nil {
		// Nothing was released; retry settlement shortly.
		r.logf("GAP_SETTLE_COMMIT_FAILED topic=%s key=%s basis=timeout err=%v", st.tk.topic, st.tk.key, err)
		st.gapDeadline = time.Now().Add(p.MaxWait)
		r.armGapTimerLocked(st, p)
		return
	}
	r.deliverBatchLocked(msgs)
}

// drainLocked releases the contiguous parked run held[start], start+1, ... It
// does not modify nextSeq; callers set the frontier themselves. Used after an
// in-order accept (start == nextSeq) and after a Continue settlement
// (start == first parked seq, above the declared-loss prefix).
func (r *Router) drainLocked(st *keyState, start uint64) []*Message {
	var out []*Message
	for seq := start; ; seq++ {
		h, ok := st.held[seq]
		if !ok {
			break
		}
		out = append(out, h.msg)
		r.retainLocked(st, h.msg)
		delete(st.held, seq)
	}
	if len(st.held) == 0 {
		st.held = nil
	}
	return out
}

func minHeldSeq(held map[uint64]*heldEntry) uint64 {
	first := true
	var min uint64
	for seq := range held {
		if first || seq < min {
			min, first = seq, false
		}
	}
	return min
}

// settleUnfillable declares the contiguous missing prefix [nextSeq, minHeld)
// permanently lost and applies the configured action. The conclusion is
// journaled before any state changes or delivery, so a commit failure fails
// closed (nothing released, nothing forgotten). Caller holds the lock and must
// deliver the returned messages after.
func (r *Router) settleUnfillableLocked(st *keyState, p ReorderPolicy, basis GapBasis, waited time.Duration) ([]*Message, error) {
	low := st.nextSeq
	high := minHeldSeq(st.held) // first parked seq; missing prefix ends below it

	missing := seqRange(low, high) // [low, high)

	var ended []uint64
	// For Continue, release the contiguous parked run starting at high until
	// the next still-missing seq; the frontier ends at the first unparked gap.
	released := make([]uint64, 0)
	newFrontier := high
	switch p.OnUnfillable {
	case GapTerminate:
		ended = make([]uint64, 0, len(st.held))
		for seq := range st.held {
			ended = append(ended, seq)
		}
		sort.Slice(ended, func(i, j int) bool { return ended[i] < ended[j] })
	case GapContinue:
		for cursor := high; ; cursor++ {
			if _, ok := st.held[cursor]; !ok {
				newFrontier = cursor
				break
			}
			released = append(released, cursor)
		}
	}

	rec := gapRecord{
		Topic:    st.tk.topic,
		Key:      st.tk.key,
		Basis:    string(basis),
		Action:   int(p.OnUnfillable),
		Missing:  missing,
		Released: released,
		Ended:    ended,
		NextSeq:  newFrontier,
	}

	if err := r.journal.AppendGapEvent(rec); err != nil {
		return nil, err
	}

	// Durable commit succeeded: apply the settlement.
	r.counters.missingSeqs.Add(int64(len(missing)))
	switch basis {
	case GapCapacity:
		r.counters.gapsLostCapacity.Add(1)
	case GapTimeout:
		r.counters.gapsLostTimeout.Add(1)
	}

	var deliver []*Message
	switch p.OnUnfillable {
	case GapTerminate:
		for _, h := range st.held {
			r.noteLostSeqLocked(st, h.msg.Seq)
		}
		st.held = nil
		st.terminated = true
		st.nextSeq = newFrontier
		r.stopGapTimerLocked(st)
		r.counters.streamsEnded.Add(1)
		r.counters.heldNow.Add(-int64(len(ended)))
	case GapContinue:
		// Release the contiguous parked run above the lost prefix and advance
		// the frontier past both the lost prefix and the released run. heldNow
		// is decremented in deliverLocked when the messages are accepted.
		deliver = r.drainLocked(st, high)
		st.nextSeq = newFrontier
		if len(st.held) > 0 {
			// Still parked behind a deeper gap: deadline follows the oldest
			// still-parked message.
			oldest := st.held[minHeldSeq(st.held)].heldAt
			st.gapDeadline = oldest.Add(p.MaxWait)
			r.armGapTimerLocked(st, p)
		} else {
			r.stopGapTimerLocked(st)
		}
	}

	ev := GapEvent{
		Topic:    st.tk.topic,
		Key:      st.tk.key,
		Basis:    basis,
		Action:   p.OnUnfillable,
		Missing:  missing,
		Released: released,
		Ended:    ended,
		Wait:     waited,
	}
	r.emitGapLocked(ev)
	return deliver, nil
}

// noteLostSeqLocked remembers a seq declared lost so an equally-late retransmit
// is distinguished (seq_gap_lost) from a normal rollback within the dedup
// window.
func (r *Router) noteLostSeqLocked(st *keyState, m uint64) {
	if st.recent == nil {
		st.recent = make(map[string]uint64)
	}
	id := fmt.Sprintf("\x00lost:%d", m)
	st.recent[id] = m
	st.order = append(st.order, m)
	if len(st.order) > dedupWindow {
		victim := st.order[0]
		st.order = st.order[1:]
		for pid, seq := range st.recent {
			if seq == victim {
				delete(st.recent, pid)
				break
			}
		}
	}
}

func isLostSeqLocked(st *keyState, seq uint64) bool {
	_, ok := st.recent[fmt.Sprintf("\x00lost:%d", seq)]
	return ok
}

func seqRange(low, high uint64) []uint64 {
	if high <= low {
		return nil
	}
	out := make([]uint64, 0, high-low)
	for s := low; s < high; s++ {
		out = append(out, s)
	}
	return out
}

func (r *Router) emitGapLocked(ev GapEvent) {
	r.logf("GAP_SETTLED topic=%s key=%s basis=%s action=%s missing=%v released=%v ended=%v wait=%s",
		ev.Topic, ev.Key, ev.Basis, ev.Action, ev.Missing, ev.Released, ev.Ended, ev.Wait)
	if r.onGap != nil {
		r.onGap(ev)
	}
}

// restoreReorder runs once after journal replay. It requires every recovered
// stream with reorder state (parked messages or a terminated frontier) to still
// have an enabled policy; otherwise recovery is refused rather than silently
// changing semantics. Surviving open gaps are re-armed with the SAME deadline
// implied by the original park time, so restart does not extend the wait.
func (r *Router) restoreReorder() error {
	for _, sh := range r.shards {
		sh.mu.Lock()
		for _, st := range sh.state {
			if len(st.held) == 0 && !st.terminated {
				continue
			}
			p, ok := r.reorderPolicyFor(st)
			if !ok {
				sh.mu.Unlock()
				return fmt.Errorf("%w: reorder state recovered for topic=%s key=%s but selector no longer enables a policy; refusing unsafe recovery",
					ErrJournalCorrupt, st.tk.topic, st.tk.key)
			}
			if st.terminated {
				continue
			}
			oldest := st.held[minHeldSeq(st.held)].heldAt
			st.gapDeadline = oldest.Add(p.MaxWait)
			if time.Until(st.gapDeadline) <= 0 {
				// Already past the deadline at boot: settle immediately.
				go r.onGapTimeout(st)
				continue
			}
			r.armGapTimerLocked(st, p)
		}
		sh.mu.Unlock()
	}
	return nil
}

// stopAllGapTimers cancels every pending gap timer (router shutdown).
func (r *Router) stopAllGapTimers() {
	for _, sh := range r.shards {
		sh.mu.Lock()
		for _, st := range sh.state {
			r.stopGapTimerLocked(st)
		}
		sh.mu.Unlock()
	}
}

// deliverLocked accepts and fans out one message while the shard lock is
// held: counts it, logs topic/seq/basis and offers it non-blockingly to every
// matching subscriber. A slow subscriber only takes a back-pressure drop and
// can never block the publisher or other subscribers. res may be nil (the
// message is a parked message released by someone else's publish/settlement).
func (r *Router) deliverLocked(res *PublishResult, msg *Message, held bool) {
	r.counters.accepted.Add(1)
	if held {
		r.counters.releasedFromHold.Add(1)
		r.counters.heldNow.Add(-1)
		r.logf("RELEASED topic=%s key=%s seq=%d id=%s basis=released from reorder hold in order",
			msg.Topic, safeKey(msg), msg.Seq, msg.ID)
	} else {
		r.logf("ACCEPTED topic=%s key=%s seq=%d id=%s basis=(seq==expected %d, id unseen)",
			msg.Topic, safeKey(msg), msg.Seq, msg.ID, msg.Seq)
	}
	for _, s := range r.matchingSubscribers(msg.Topic) {
		if s.offer(msg) {
			if res != nil {
				res.Delivered++
			}
			continue
		}
		if res != nil {
			res.Dropped++
		}
		r.counters.droppedBackpressure.Add(1)
		r.logf("BACKPRESSURE topic=%s seq=%d id=%s subscriber=%s policy=%s",
			msg.Topic, msg.Seq, msg.ID, s.id, s.policy)
	}
}

// deliverBatchLocked delivers a batch of messages released by a timer-driven
// settlement, strictly in ascending order.
func (r *Router) deliverBatchLocked(msgs []*Message) {
	for _, msg := range msgs {
		r.deliverLocked(nil, msg, true)
	}
}

// publishReorderLocked is the Publish decision for a key with a reorder
// policy. Caller holds the shard lock. The returned batch must be fan-out in
// order by the caller (also under the shard lock); it contains the triggering
// message when it is accepted/delivered now.
func (r *Router) publishReorderLocked(st *keyState, p ReorderPolicy, msg *Message) (PublishResult, []*Message) {
	detail := func(f string) string {
		return fmt.Sprintf("topic=%s key=%s seq=%d id=%s: %s", msg.Topic, safeKey(msg), msg.Seq, msg.ID, f)
	}

	switch {
	case st.terminated:
		return r.reorderReject(msg, RejectStreamTerminated,
			detail(fmt.Sprintf("partition stream terminated after unfillable gap; next expected seq=%d", st.nextSeq))), nil

	case st.recent[msg.ID] != 0:
		return r.reorderReject(msg, RejectDuplicate,
			fmt.Sprintf("topic=%s key=%s seq=%d id=%s: message id already consumed/parked at seq=%d; not redelivered",
				msg.Topic, safeKey(msg), msg.Seq, msg.ID, st.recent[msg.ID])), nil

	case isLostSeqLocked(st, msg.Seq):
		return r.reorderReject(msg, RejectGapLost,
			detail("seq was declared permanently unfillable after capacity/wait bound; not reordered")), nil

	case msg.Seq < st.nextSeq:
		return r.reorderReject(msg, RejectRollback,
			detail(fmt.Sprintf("seq behind expected=%d; rejected, not delivered", st.nextSeq))), nil
	}

	// seq >= nextSeq from here. A position already parked by another message
	// is a producer contradiction (the seq can only be filled by that parked
	// message); the existing message is kept, never overwritten.
	if h := st.held[msg.Seq]; h != nil && msg.Seq >= st.nextSeq {
		return r.reorderReject(msg, RejectHeldConflict,
			detail(fmt.Sprintf("seq already parked by id=%s; existing message kept, not overwritten", h.msg.ID))), nil
	}

	if msg.Seq > st.nextSeq {
		// Open gap: park the out-of-order message within the bounded window.
		if len(st.held) >= p.MaxHeld {
			// Capacity bound reached: settle the gap before admitting more.
			waited := time.Since(st.held[minHeldSeq(st.held)].heldAt)
			released, err := r.settleUnfillableLocked(st, p, GapCapacity, waited)
			if err != nil {
				return PublishResult{
					Accepted: false, Reason: "journal_error", Detail: err.Error(), Message: msg,
				}, nil
			}
			// Settlement may have Continue'd the frontier. Retry the same
			// message against the new state (terminate => rejected, admission
			// => released in order, deeper gap => parked).
			res, batch := r.publishReorderLocked(st, p, msg)
			batch = append(released, batch...)
			return res, batch
		}
		if err := r.parkLocked(st, p, msg, time.Now()); err != nil {
			return PublishResult{
				Accepted: false, Reason: "journal_error", Detail: err.Error(), Message: msg,
			}, nil
		}
		r.logf("HELD topic=%s key=%s seq=%d id=%s basis=gap open, expected=%d; parked %d/%d, deadline=%s",
			msg.Topic, safeKey(msg), msg.Seq, msg.ID, st.nextSeq, len(st.held), p.MaxHeld, st.gapDeadline.Format(time.RFC3339Nano))
		return PublishResult{Accepted: true, Held: true, Reason: RejectHeld, Message: msg}, nil
	}

	// seq == nextSeq, id unseen: commit, advance, then release any contiguous
	// parked tail in original order.
	if err := r.journal.AppendMessage(msg); err != nil {
		return PublishResult{
			Accepted: false, Reason: "journal_error", Detail: err.Error(), Message: msg,
		}, nil
	}
	st.advance(msg)
	r.retainLocked(st, msg)

	batch := []*Message{msg}
	hadGap := len(st.held) > 0
	if hadGap {
		released := r.drainLocked(st, msg.Seq+1)
		if len(released) > 0 {
			st.nextSeq = released[len(released)-1].Seq + 1
		}
		batch = append(batch, released...)
		if len(st.held) == 0 {
			r.stopGapTimerLocked(st)
			r.counters.gapsFilled.Add(1)
			r.emitGapLocked(GapEvent{
				Topic:    st.tk.topic,
				Key:      st.tk.key,
				Basis:    GapFilled,
				Released: seqRangeFor(released),
			})
		} else {
			oldest := st.held[minHeldSeq(st.held)].heldAt
			st.gapDeadline = oldest.Add(p.MaxWait)
			r.armGapTimerLocked(st, p)
		}
	}
	return PublishResult{Accepted: true, Message: msg}, batch
}

func seqRangeFor(msgs []*Message) []uint64 {
	out := make([]uint64, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.Seq)
	}
	return out
}
