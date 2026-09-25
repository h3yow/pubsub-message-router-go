package router

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
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
	// nextSeq is the expected Seq of the next new message.
	nextSeq uint64
	// recent maps message ID -> Seq for bounded duplicate detection beyond
	// the last-delivered Seq (a producer may resend any old id, not just seq-1).
	recent map[string]uint64
	// order records recent seq values in arrival order (ring eviction).
	order []uint64
	// retained are accepted messages kept for durable-subscriber replay.
	retained []*Message
}

const dedupWindow = 4096

func newKeyState() *keyState {
	return &keyState{nextSeq: 1, recent: make(map[string]uint64)}
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
		st = newKeyState()
		shard.state[k] = st
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
	}
	shard.mu.Unlock()

	if !res.Accepted {
		return res
	}

	r.counters.accepted.Add(1)
	r.logf("ACCEPTED topic=%s key=%s seq=%d id=%s basis=(seq==expected %d, id unseen)",
		msg.Topic, k.key, msg.Seq, msg.ID, msg.Seq)

	// Fan-out: non-blocking per subscriber. A slow subscriber can never
	// block the publisher or any other subscriber.
	for _, s := range r.matchingSubscribers(msg.Topic) {
		if s.offer(msg) {
			res.Delivered++
		} else {
			res.Dropped++
			r.counters.droppedBackpressure.Add(1)
			r.logf("BACKPRESSURE topic=%s seq=%d id=%s subscriber=%s policy=%s",
				msg.Topic, msg.Seq, msg.ID, s.id, s.policy)
		}
	}
	return res
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
// key frontier and retained history; cursor records (last one wins per
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
			m := e.msg
			if err := m.Validate(); err != nil {
				return fmt.Errorf("journal holds invalid message: %w", err)
			}
			k := m.partitionKey()
			sh := r.shardFor(k)
			sh.mu.Lock()
			st, ok := sh.state[k]
			if !ok {
				st = newKeyState()
				sh.state[k] = st
			}
			st.advance(m)
			r.retainLocked(st, m)
			sh.mu.Unlock()
			r.counters.replayed.Add(1)
		}
		return nil
	})
	if err != nil {
		return err
	}
	r.recoveredCursors = cursors
	r.logf("RECOVERY messages=%d durable_subscribers=%d",
		r.counters.replayed.Load(), len(cursors))
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
