package router

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"
)

// defaultShardCount shards per-key ordering state to reduce lock contention
// across many concurrent publishers.
const defaultShardCount = 32

// Config configures a Router.
type Config struct {
	// Journal durably persists accepted messages and durable cursors.
	// When nil (or in-memory), restart guarantees do not apply.
	Journal *Journal
	// ShardCount controls lock sharding; defaults to 32.
	ShardCount int
	// Logger receives one line per publish disposition with topic, seq and
	// the decision basis. nil disables logging.
	Logger Logger
	// RetainPerKey bounds how many accepted messages are kept per partition
	// key for replay to late/recovering durable subscribers. 0 = unlimited.
	RetainPerKey int
	// Reorder, when non-nil, enables bounded out-of-order buffering per
	// partition key for the keys it returns a policy for. Keys without a
	// policy keep the strict-contiguous semantics unchanged.
	Reorder ReorderSelector
	// OnGapEvent receives one event per gap conclusion (fill / capacity /
	// timeout). May be nil.
	OnGapEvent GapCallback
}

// Logger is the minimal logging surface used by the router.
type Logger interface {
	Printf(format string, args ...any)
}

// Router routes locally published messages to matching subscribers.
type Router struct {
	journal *Journal
	logger  Logger
	retain  int
	reorder ReorderSelector
	onGap   GapCallback

	shards []*keyShard
	subMu  sync.RWMutex
	subs   map[string]*subscriber // id -> subscriber

	// recoveredCursors holds durable subscriber cursors loaded at startup:
	// subscriberID -> partition key -> acknowledged seq. Consulted when a
	// durable subscriber (re)registers.
	recoveredCursors map[string]map[topicKey]uint64

	counters Counters
	ctx      context.Context
	cancel   context.CancelFunc

	closeMu sync.Mutex
	closed  bool
	wg      sync.WaitGroup
}

// keyShard guards the ordering/dedup state of a set of partition keys.
type keyShard struct {
	mu    sync.Mutex
	state map[topicKey]*keyState
}

// keyState is the per (topic,key) stream state.
type keyState struct {
	// tk identifies the partition; needed because states are recovered from
	// the journal before any publish touches the key.
	tk topicKey
	// nextSeq is the expected Seq of the next new message.
	nextSeq uint64
	// recent maps message ID -> Seq for bounded duplicate detection beyond
	// the last-delivered Seq (a producer may resend any old id, not just seq-1).
	recent map[string]uint64
	// order records recent seq values in arrival order (ring eviction).
	order []uint64
	// retained are accepted messages kept for durable-subscriber replay.
	retained []*Message

	// ---- bounded reorder (only used when a policy applies to this key) ----

	// policy is non-nil when reorder buffering is enabled for the key.
	policy *ReorderPolicy
	// policyChecked caches that the selector was consulted for this state.
	policyChecked bool
	// held parks out-of-order messages keyed by seq while a gap is open.
	held map[uint64]*heldEntry
	// gapDeadline is oldest heldAt + MaxWait while a gap is open.
	gapDeadline time.Time
	// timer fires the wait-bound settlement; nil when no gap is open.
	timer *time.Timer
	// terminated means the stream failed closed after an unfillable gap.
	terminated bool
}

const dedupWindow = 4096

func newKeyState(tk topicKey) *keyState {
	return &keyState{tk: tk, nextSeq: 1, recent: make(map[string]uint64)}
}

// New constructs a router and replays the journal when one is configured.
// Replay rebuilds per-key sequence state and durable subscriber cursors so
// that post-restart publishes are checked against the recovered frontier.
func New(cfg Config) (*Router, error) {
	n := cfg.ShardCount
	if n <= 0 {
		n = defaultShardCount
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Router{
		journal: cfg.Journal,
		logger:  cfg.Logger,
		retain:  cfg.RetainPerKey,
		reorder: cfg.Reorder,
		onGap:   cfg.OnGapEvent,
		shards:  make([]*keyShard, n),
		subs:    make(map[string]*subscriber),
		ctx:     ctx,
		cancel:  cancel,
	}
	for i := range r.shards {
		r.shards[i] = &keyShard{state: make(map[topicKey]*keyState)}
	}
	if r.journal != nil && r.journal.Enabled() {
		if err := r.recoverFromJournal(); err != nil {
			cancel()
			return nil, err
		}
	}
	return r, nil
}

func (r *Router) shardFor(k topicKey) *keyShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(k.topic))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(k.key))
	return r.shards[int(h.Sum32())%len(r.shards)]
}

// ---- subscription ---------------------------------------------------------

// subscribeOptions holds subscription settings.
type subscribeOptions struct {
	id             string
	bufferSize     int
	overflowPolicy OverflowPolicy
	onOverflow     OverflowCallback
	durable        bool
}

// SubscribeOption mutates subscribe options.
type SubscribeOption func(*subscribeOptions)

// WithBufferSize sets the bounded per-subscriber delivery buffer.
func WithBufferSize(n int) SubscribeOption {
	return func(o *subscribeOptions) { o.bufferSize = n }
}

// WithOverflowPolicy sets the explicit overflow policy.
func WithOverflowPolicy(p OverflowPolicy) SubscribeOption {
	return func(o *subscribeOptions) { o.overflowPolicy = p }
}

// WithOverflowCallback registers an observable overflow event sink.
func WithOverflowCallback(cb OverflowCallback) SubscribeOption {
	return func(o *subscribeOptions) { o.onOverflow = cb }

}

// WithDurable makes a subscriber durable: its acked cursor persists in the
// journal and, after a router restart, unacknowledged retained messages are
// replayed to it (at-least-once; handlers must be idempotent by message ID).
func WithDurable(id string) SubscribeOption {
	return func(o *subscribeOptions) { o.durable = true; o.id = id }
}

// Subscribe registers a handler for a topic filter (wildcards allowed).
// A new random subscriber id is assigned for non-durable subscriptions.
func (r *Router) Subscribe(filter string, h HandlerFunc, opts ...SubscribeOption) (*Subscription, error) {
	if strings.TrimSpace(filter) == "" {
		return nil, fmt.Errorf("empty subscription filter")
	}
	if strings.Contains(filter, "#/") {
		return nil, fmt.Errorf("invalid filter %q: '#' is only valid as the last segment", filter)
	}
	o := subscribeOptions{overflowPolicy: OverflowDropNewest, bufferSize: 16}
	for _, opt := range opts {
		opt(&o)
	}
	r.closeMu.Lock()
	if r.closed {
		r.closeMu.Unlock()
		return nil, fmt.Errorf("router closed")
	}
	r.subMu.Lock()
	if o.id == "" {
		o.id = fmt.Sprintf("sub-%d-%d", r.counters.subscribersLive.Load()+1, newSubSeq())
	}
	if _, exists := r.subs[o.id]; exists {
		r.subMu.Unlock()
		r.closeMu.Unlock()
		return nil, fmt.Errorf("subscriber id %q already registered", o.id)
	}
	s := newSubscriber(r, o.id, filter, h, o)
	r.subs[o.id] = s
	r.subMu.Unlock()
	r.closeMu.Unlock()

	r.counters.subscribersLive.Add(1)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		s.run(r.ctx)
	}()

	// Durable subscriber joining after start: replay retained messages it
	// has not yet acknowledged, in order.
	if s.durable {
		r.replayToSubscriber(s)
	}
	return &Subscription{sub: s, r: r}, nil
}

var subSeqCounter uint64

func newSubSeq() uint64 {
	subSeqCounter++
	return subSeqCounter
}

func (r *Router) unsubscribe(id string) {
	r.subMu.Lock()
	s, ok := r.subs[id]
	if ok {
		delete(r.subs, id)
	}
	r.subMu.Unlock()
	if !ok {
		return
	}
	r.counters.subscribersLive.Add(-1)
	s.stop()
}

func (r *Router) matchingSubscribers(topic string) []*subscriber {
	r.subMu.RLock()
	out := make([]*subscriber, 0, len(r.subs))
	for _, s := range r.subs {
		if MatchTopic(s.filter, topic) {
			out = append(out, s)
		}
	}
	r.subMu.RUnlock()
	return out
}

// ---- publishing -----------------------------------------------------------

// publishOptions holds publish settings.
type publishOptions struct{}

// PublishOption mutates publish behavior (reserved for future use).
type PublishOption func(*publishOptions)

// Publish validates, orders, durably commits and fans out one message.
//
// Per partition key the Seq frontier is checked under a shard lock:
//   - ID already seen (or Seq == last with a *different* id) -> duplicate,
//     explicitly reported, never redelivered;
//   - Seq < expected frontier                          -> seq_rollback;
//   - Seq > expected frontier                          -> seq_gap
//     (discontinuous sequence, rejected rather than silently buffered);
//   - Seq == expected frontier and ID new               -> accepted.
//
// Accepted messages are journaled before fan-out, then offered to each
// matching subscriber via non-blocking bounded buffers.
func (r *Router) Publish(ctx context.Context, msg *Message, _ ...PublishOption) PublishResult {
	r.counters.published.Add(1)
	r.closeMu.Lock()
	closed := r.closed
	r.closeMu.Unlock()
	if closed {
		return r.reject(msg, RejectRouterClosed, "router is closed", nil)
	}
	if err := msg.Validate(); err != nil {
		er, _ := AsReject(err)
		return r.reject(msg, er.Reason, er.Detail, nil)
	}

	k := msg.partitionKey()
	shard := r.shardFor(k)

	shard.mu.Lock()
	st, ok := shard.state[k]
	if !ok {
		st = newKeyState(k)
		shard.state[k] = st
	}

	// Keys with a reorder policy: out-of-order messages are parked within
	// bounded capacity/wait instead of being rejected immediately. Keys
	// without a policy take the historical strict-contiguous path unchanged.
	if p, enabled := r.reorderPolicyFor(st); enabled {
		res, batch := r.publishReorderLocked(st, p, msg)
		// Fan-out remains under the shard lock so decision order equals
		// delivery order per key; offers are non-blocking, so a slow
		// subscriber still cannot block the publisher or anyone else.
		for _, m := range batch {
			var target *PublishResult
			if m == msg {
				target = &res
			}
			// Every batch member other than the triggering message was parked;
			// the triggering message itself is a fresh in-order acceptance.
			r.deliverLocked(target, m, m != msg)
		}
		shard.mu.Unlock()
		return res
	}

	var res PublishResult
	switch {
	case st.recent[msg.ID] != 0:
		res = r.reject(msg, RejectDuplicate,
			fmt.Sprintf("topic=%s key=%s seq=%d id=%s: message id already consumed at seq=%d; not redelivered",
				msg.Topic, msg.Key, msg.Seq, msg.ID, st.recent[msg.ID]), st)
	case msg.Seq < st.nextSeq:
		// Old position with an unseen id: sequence number regression.
		res = r.reject(msg, RejectRollback,
			fmt.Sprintf("topic=%s key=%s seq=%d id=%s: seq behind expected=%d; rejected, not delivered",
				msg.Topic, msg.Key, msg.Seq, msg.ID, st.nextSeq), st)
	case msg.Seq > st.nextSeq:
		res = r.reject(msg, RejectGap,
			fmt.Sprintf("topic=%s key=%s seq=%d id=%s: discontinuity, expected=%d; reject rather than silently skip",
				msg.Topic, msg.Key, msg.Seq, msg.ID, st.nextSeq), st)
	default:
		// New, in-order message: commit durable state under the shard lock
		// so concurrent publishers on the same key are serialized.
		if r.journal != nil {
			if err := r.journal.AppendMessage(msg); err != nil {
				shard.mu.Unlock()
				res = PublishResult{Accepted: false, Reason: "journal_error", Detail: err.Error(), Message: msg}
				r.logf("REJECTED topic=%s seq=%d id=%s reason=journal_error detail=%v", msg.Topic, msg.Seq, msg.ID, err)
				r.counters.rejected.Add(1)
				return res
			}
		}
		st.advance(msg)
		r.retainLocked(st, msg)
		res = PublishResult{Accepted: true, Message: msg}
		r.deliverLocked(&res, msg, false)
	}
	shard.mu.Unlock()

	return res
}

// reorderReject records a reorder-specific rejection conclusion. Each reason
// has its own counter; stream_terminated is a lifecycle verdict and not added
// to the aggregate rejected count.
func (r *Router) reorderReject(msg *Message, reason RejectReason, detail string) PublishResult {
	switch reason {
	case RejectDuplicate:
		r.counters.duplicates.Add(1)
		r.counters.rejected.Add(1)
	case RejectRollback:
		r.counters.seqRollbacks.Add(1)
		r.counters.rejected.Add(1)
	case RejectHeldConflict:
		r.counters.heldConflicts.Add(1)
		r.counters.rejected.Add(1)
	case RejectGapLost:
		r.counters.gapLostRejects.Add(1)
		r.counters.rejected.Add(1)
	case RejectStreamTerminated:
		r.counters.streamRejects.Add(1)
	}
	r.logf("REJECTED topic=%s key=%s seq=%d id=%s reason=%s detail=%s",
		safeTopic(msg), safeKey(msg), safeSeq(msg), safeID(msg), reason, detail)
	return PublishResult{Accepted: false, Reason: reason, Detail: detail, Message: msg}
}

func (st *keyState) advance(msg *Message) {
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
	st.nextSeq = msg.Seq + 1
}

func (r *Router) retainLocked(st *keyState, msg *Message) {
	if r.retain <= 0 {
		return
	}
	st.retained = append(st.retained, msg)
	if len(st.retained) > r.retain {
		st.retained = st.retained[len(st.retained)-r.retain:]
	}
}

func (r *Router) reject(msg *Message, reason RejectReason, detail string, st *keyState) PublishResult {
	switch reason {
	case RejectDuplicate:
		r.counters.duplicates.Add(1)
	case RejectRollback:
		r.counters.seqRollbacks.Add(1)
	case RejectGap:
		r.counters.seqGaps.Add(1)
	}
	if reason != RejectRouterClosed {
		r.counters.rejected.Add(1)
	}
	effKey := safeKey(msg)
	r.logf("REJECTED topic=%s key=%s seq=%d id=%s reason=%s detail=%s",
		safeTopic(msg), effKey, safeSeq(msg), safeID(msg), reason, detail)
	return PublishResult{Accepted: false, Reason: reason, Detail: detail, Message: msg}
}

func safeTopic(m *Message) string {
	if m == nil {
		return ""
	}
	return m.Topic
}
func safeSeq(m *Message) uint64 {
	if m == nil {
		return 0
	}
	return m.Seq
}
func safeID(m *Message) string {
	if m == nil {
		return ""
	}
	return m.ID
}

// safeKey returns the effective partition key (explicit key, else topic).
func safeKey(m *Message) string {
	if m == nil {
		return ""
	}
	if m.Key != "" {
		return m.Key
	}
	return m.Topic
}

func (r *Router) logf(format string, args ...any) {
	if r.logger != nil {
		r.logger.Printf(format, args...)
	}
}

// ---- durability -----------------------------------------------------------

// cursorEntry tracks one durable subscriber's acknowledged seq per key.
type cursorEntry struct {
	ackSeq uint64
}

func (r *Router) advanceCursor(subID string, k topicKey, seq uint64) error {
	if r.journal == nil || !r.journal.Enabled() {
		return nil
	}
	return r.journal.AppendCursor(subID, k.topic, k.key, seq)
}

// recoverFromJournal replays the log at startup: accepted messages rebuild
// key frontier and retained history; hold records restore messages still
// parked behind an open gap; gap records restore unfillable-gap conclusions
// (frontier jumps / terminated streams); cursor records (last one wins per
// subscriber) set the replay starting point.
func (r *Router) recoverFromJournal() error {
	cursors := make(map[string]map[topicKey]uint64)
	err := r.journal.Replay(func(e journalEntry) error {
		switch e.kind {
		case journalKindCursor:
			cm, ok := cursors[e.subID]
			if !ok {
				cm = make(map[topicKey]uint64)
				cursors[e.subID] = cm
			}
			cm[topicKey{topic: e.topic, key: e.key}] = e.seq
		case journalKindMessage:
			if err := r.recoverMessageLocked(e.msg); err != nil {
				return err
			}
			r.counters.replayed.Add(1)
		case journalKindHold:
			if err := r.recoverHoldLocked(e); err != nil {
				return err
			}
			r.counters.replayed.Add(1)
		case journalKindGap:
			if err := r.recoverGapLocked(e.gap); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := r.restoreReorder(); err != nil {
		return err
	}
	r.recoveredCursors = cursors
	r.logf("RECOVERY messages=%d durable_subscribers=%d",
		r.counters.replayed.Load(), len(cursors))
	return nil
}

// stateForLocked returns the key state for m, creating it if needed. Caller
// does not need to hold any lock yet (the shard lock is taken internally).
func (r *Router) stateForLocked(k topicKey) (*keyShard, *keyState) {
	sh := r.shardFor(k)
	sh.mu.Lock()
	st, ok := sh.state[k]
	if !ok {
		st = newKeyState(k)
		sh.state[k] = st
	}
	return sh, st
}

// recoverMessageLocked replays one committed (kind 1) message. A kind-1 seq
// must be exactly the next contiguous seq; a kind-1 for a still-parked seq
// would mean a hold frame was never settled — an incomplete/unsafe state.
func (r *Router) recoverMessageLocked(m *Message) error {
	if err := m.Validate(); err != nil {
		return fmt.Errorf("journal holds invalid message: %w", err)
	}
	k := m.partitionKey()
	sh, st := r.stateForLocked(k)
	defer sh.mu.Unlock()
	if st.terminated {
		return fmt.Errorf("%w: committed message topic=%s key=%s seq=%d after terminated stream",
			ErrJournalCorrupt, k.topic, k.key, m.Seq)
	}
	if _, parked := st.held[m.Seq]; parked {
		return fmt.Errorf("%w: committed topic=%s key=%s seq=%d duplicates a still-held record",
			ErrJournalCorrupt, k.topic, k.key, m.Seq)
	}
	if m.Seq != st.nextSeq {
		return fmt.Errorf("%w: committed topic=%s key=%s seq=%d out of order, expected=%d",
			ErrJournalCorrupt, k.topic, k.key, m.Seq, st.nextSeq)
	}
	st.advance(m)
	r.retainLocked(st, m)
	// A committed message may release a contiguous run of parked messages.
	if len(st.held) > 0 {
		released := r.drainLocked(st, m.Seq+1)
		if len(released) > 0 {
			st.nextSeq = released[len(released)-1].Seq + 1
		}
		if len(st.held) == 0 {
			r.stopGapTimerLocked(st)
		}
	}
	return nil
}

// recoverHoldLocked restores one still-parked message (kind 4).
func (r *Router) recoverHoldLocked(e journalEntry) error {
	m := e.msg
	if err := m.Validate(); err != nil {
		return fmt.Errorf("journal holds invalid held message: %w", err)
	}
	k := m.partitionKey()
	if e.topic != k.topic || e.key != k.key || e.heldAt.IsZero() {
		return fmt.Errorf("%w: hold record partition/held_at mismatch for topic=%s seq=%d",
			ErrJournalCorrupt, k.topic, m.Seq)
	}
	sh, st := r.stateForLocked(k)
	defer sh.mu.Unlock()
	if st.terminated {
		return fmt.Errorf("%w: held record topic=%s key=%s seq=%d after terminated stream",
			ErrJournalCorrupt, k.topic, k.key, m.Seq)
	}
	if m.Seq <= st.nextSeq {
		return fmt.Errorf("%w: held topic=%s key=%s seq=%d must be above frontier=%d",
			ErrJournalCorrupt, k.topic, k.key, m.Seq, st.nextSeq)
	}
	if _, dup := st.held[m.Seq]; dup {
		return fmt.Errorf("%w: duplicate held record topic=%s key=%s seq=%d",
			ErrJournalCorrupt, k.topic, k.key, m.Seq)
	}
	if st.held == nil {
		st.held = make(map[uint64]*heldEntry)
	}
	st.held[m.Seq] = &heldEntry{msg: m, heldAt: e.heldAt}
	st.recent[m.ID] = m.Seq
	st.order = append(st.order, m.Seq)
	r.counters.heldNow.Add(1)
	return nil
}

// recoverGapLocked restores one gap conclusion (kind 3) and verifies it
// against the rebuilt hold set: the journal must be internally consistent.
func (r *Router) recoverGapLocked(g *gapRecord) error {
	k := topicKey{topic: g.Topic, key: g.Key}
	sh, st := r.stateForLocked(k)
	defer sh.mu.Unlock()

	basis := GapBasis(g.Basis)
	action := GapAction(g.Action)
	if basis != GapCapacity && basis != GapTimeout {
		return fmt.Errorf("%w: gap record topic=%s key=%s has bad basis=%q",
			ErrJournalCorrupt, g.Topic, g.Key, g.Basis)
	}
	if action != GapContinue && action != GapTerminate {
		return fmt.Errorf("%w: gap record topic=%s key=%s has bad action=%d",
			ErrJournalCorrupt, g.Topic, g.Key, g.Action)
	}

	// Missing seqs must be the contiguous prefix [nextSeq, minHeld).
	high := st.nextSeq + uint64(len(g.Missing))
	for i, s := range g.Missing {
		if s != st.nextSeq+uint64(i) {
			return fmt.Errorf("%w: gap record topic=%s key=%s missing not contiguous: %v",
				ErrJournalCorrupt, g.Topic, g.Key, g.Missing)
		}
	}
	if len(st.held) > 0 && high != minHeldSeq(st.held) {
		return fmt.Errorf("%w: gap record topic=%s key=%s missing end=%d != first held=%d",
			ErrJournalCorrupt, g.Topic, g.Key, high, minHeldSeq(st.held))
	}

	switch action {
	case GapTerminate:
		if len(g.Ended) != len(st.held) {
			return fmt.Errorf("%w: terminate gap record topic=%s key=%s ended=%d held=%d",
				ErrJournalCorrupt, g.Topic, g.Key, len(g.Ended), len(st.held))
		}
		for _, s := range g.Ended {
			h, ok := st.held[s]
			if !ok {
				return fmt.Errorf("%w: terminate gap record references unheld seq=%d",
					ErrJournalCorrupt, s)
			}
			r.noteLostSeqLocked(st, h.msg.Seq)
		}
		st.held = nil
		st.terminated = true
		st.nextSeq = g.NextSeq
	case GapContinue:
		if len(g.Released) == 0 || g.Released[0] != high {
			return fmt.Errorf("%w: continue gap record topic=%s key=%s released must start at %d",
				ErrJournalCorrupt, g.Topic, g.Key, high)
		}
		for i, s := range g.Released {
			if i > 0 && s != g.Released[i-1]+1 {
				return fmt.Errorf("%w: continue gap record released not contiguous: %v",
					ErrJournalCorrupt, g.Released)
			}
			if _, ok := st.held[s]; !ok {
				return fmt.Errorf("%w: continue gap record references unheld seq=%d",
					ErrJournalCorrupt, s)
			}
		}
		lastReleased := g.Released[len(g.Released)-1]
		if g.NextSeq != lastReleased+1 {
			return fmt.Errorf("%w: continue gap record nextSeq=%d must follow last released=%d",
				ErrJournalCorrupt, g.NextSeq, lastReleased)
		}
		// The recorded frontier must land on a still-missing seq (or on the
		// first still-parked seq of a deeper gap); it must not silently skip
		// past messages still present in the hold set.
		if _, parked := st.held[g.NextSeq]; parked {
			return fmt.Errorf("%w: continue gap record nextSeq=%d collides with held message",
				ErrJournalCorrupt, g.NextSeq)
		}
		// Apply the same jump+release as a live Continue settlement: release
		// the contiguous parked run above the lost prefix, then advance.
		released := r.drainLocked(st, high)
		st.nextSeq = g.NextSeq
		if uint64(len(released)) != uint64(len(g.Released)) {
			return fmt.Errorf("%w: continue gap record released=%d drained=%d",
				ErrJournalCorrupt, len(g.Released), len(released))
		}
	}
	return nil
}

// replayToSubscriber delivers retained messages a durable subscriber has not
// acknowledged. Delivered through the same bounded buffer, preserving order;
// failure to enqueue follows the configured overflow policy (counted).
func (r *Router) replayToSubscriber(s *subscriber) {
	cursors := r.recoveredCursors[s.id]
	var replayed int
	for _, sh := range r.shards {
		sh.mu.Lock()
		keys := make([]topicKey, 0, len(sh.state))
		for k := range sh.state {
			keys = append(keys, k)
		}
		var pending []*Message
		for _, k := range keys {
			if !MatchTopic(s.filter, k.topic) {
				continue
			}
			ackSeq := cursors[k]
			for _, m := range sh.state[k].retained {
				if m.Seq > ackSeq {
					pending = append(pending, m)
				}
			}
		}
		sh.mu.Unlock()
		// pending is globally ordered per shard iteration; callers care
		// about per-key order, which retained slices preserve.
		for _, m := range pending {
			if s.offer(m) {
				replayed++
			} else {
				r.counters.droppedBackpressure.Add(1)
			}
		}
	}
	r.logf("REPLAY subscriber=%s keys=%d offered=%d", s.id, len(cursors), replayed)
}

// Snapshot returns the current observable counters.
func (r *Router) Snapshot() Stats { return r.counters.snapshot() }

// Close stops delivery goroutines and releases the journal.
func (r *Router) Close() error {
	r.closeMu.Lock()
	if r.closed {
		r.closeMu.Unlock()
		return nil
	}
	r.closed = true
	r.closeMu.Unlock()

	r.stopAllGapTimers()
	r.cancel()
	r.subMu.Lock()
	subs := make([]*subscriber, 0, len(r.subs))
	for _, s := range r.subs {
		subs = append(subs, s)
	}
	r.subs = make(map[string]*subscriber)
	r.subMu.Unlock()
	for _, s := range subs {
		s.stop()
	}
	r.wg.Wait()
	if r.journal != nil {
		return r.journal.Close()
	}
	return nil
}
