package router

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// collector subscribes to a topic and records delivered seq/ids in arrival
// order, guarded by a mutex (delivery is single-goroutine per subscriber but
// asserts run on the test goroutine).
type collector struct {
	mu   sync.Mutex
	seqs []uint64
	ids  []string
	ch   chan struct{}
}

func newCollector() *collector { return &collector{ch: make(chan struct{}, 1024)} }

func (c *collector) handler(_ context.Context, m *Message) error {
	c.mu.Lock()
	c.seqs = append(c.seqs, m.Seq)
	c.ids = append(c.ids, m.ID)
	c.mu.Unlock()
	select {
	case c.ch <- struct{}{}:
	default:
	}
	return nil
}

func (c *collector) snapshot() ([]uint64, []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	seqs := append([]uint64(nil), c.seqs...)
	ids := append([]string(nil), c.ids...)
	return seqs, ids
}

func (c *collector) waitN(t *testing.T, n int, d time.Duration) ([]uint64, []string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := len(c.seqs)
		c.mu.Unlock()
		if got >= n {
			return c.snapshot()
		}
		select {
		case <-c.ch:
		case <-time.After(5 * time.Millisecond):
		}
	}
	seqs, ids := c.snapshot()
	t.Fatalf("collector got %d messages, want %d (seqs=%v)", len(seqs), n, seqs)
	return seqs, ids
}

// assertStrictIncreasing verifies strictly increasing seqs and no duplicate ids.
func assertStrictIncreasing(t *testing.T, seqs []uint64, ids []string) {
	t.Helper()
	seenID := map[string]bool{}
	for i, s := range seqs {
		if i > 0 && s <= seqs[i-1] {
			t.Fatalf("delivered order not strictly increasing at %d: %v", i, seqs)
		}
		if ids != nil {
			if seenID[ids[i]] {
				t.Fatalf("duplicate delivery of id=%s, order=%v", ids[i], seqs)
			}
			seenID[ids[i]] = true
		}
	}
}

type gapSink struct {
	mu     sync.Mutex
	events []GapEvent
}

func (g *gapSink) add(e GapEvent) { g.mu.Lock(); g.events = append(g.events, e); g.mu.Unlock() }
func (g *gapSink) list() []GapEvent {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]GapEvent(nil), g.events...)
}

func enableReorder(p ReorderPolicy) ReorderSelector {
	return func(topic, key string) (ReorderPolicy, bool) { return p, true }
}

func reorderCfg(p ReorderPolicy, sink *gapSink) Config {
	cfg := Config{Reorder: enableReorder(p)}
	if sink != nil {
		cfg.OnGapEvent = sink.add
	}
	return cfg
}

// 1) Gap that later fills: parked messages released in original order.
func TestReorderGapFillsInOrder(t *testing.T) {
	sink := &gapSink{}
	r := testRouter(t, reorderCfg(ReorderPolicy{
		MaxHeld: 16, MaxWait: time.Second, OnUnfillable: GapContinue,
	}, sink))
	c := newCollector()
	sub, err := r.Subscribe("orders", c.handler, WithBufferSize(128))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	mustPublish(t, r, msgAt("o1", 1, "orders"))
	// Late gap: 4,5 arrive before 2,3.
	for _, seq := range []uint64{4, 5} {
		res := r.Publish(context.Background(), msgAt("o"+uitoa(seq), seq, "orders"))
		if !res.Accepted || !res.Held {
			t.Fatalf("seq=%d want held, got %+v", seq, res)
		}
	}
	if seqs, _ := c.snapshot(); len(seqs) != 1 {
		t.Fatalf("nothing above the gap may deliver, got %v", seqs)
	}
	mustPublish(t, r, msgAt("o2", 2, "orders"))
	mustPublish(t, r, msgAt("o3", 3, "orders"))

	seqs, ids := c.waitN(t, 5, time.Second)
	assertStrictIncreasing(t, seqs, ids)
	if seqs[0] != 1 || seqs[4] != 5 || len(seqs) != 5 {
		t.Fatalf("want 1..5 in order, got %v", seqs)
	}
	var filled int
	for _, e := range sink.list() {
		if e.Basis == GapFilled {
			filled++
			if len(e.Released) != 2 || e.Released[0] != 4 || e.Released[1] != 5 {
				t.Fatalf("fill released=%v", e.Released)
			}
		}
	}
	if filled != 1 {
		t.Fatalf("want exactly one fill conclusion, got %+v", sink.list())
	}
	snap := r.Snapshot()
	if snap.HeldNow != 0 || snap.GapsFilled != 1 || snap.ReleasedFromHold != 2 || snap.HeldTotal != 2 {
		t.Fatalf("counters wrong: %+v", snap)
	}
	t.Logf("topic=orders gap filled: delivered=%v; counters held=%d released=%d filled=%d",
		seqs, snap.HeldTotal, snap.ReleasedFromHold, snap.GapsFilled)
}

// Resend of a parked id while still parked is a duplicate, and a different id
// claiming the parked seq is a held conflict — distinct conclusions.
func TestReorderParkedDuplicateAndConflict(t *testing.T) {
	r := testRouter(t, reorderCfg(ReorderPolicy{
		MaxHeld: 8, MaxWait: time.Second, OnUnfillable: GapContinue,
	}, nil))
	c := newCollector()
	sub, _ := r.Subscribe("k", c.handler, WithBufferSize(64))
	defer sub.Close()

	mustPublish(t, r, msgAt("k1", 1, "k"))
	res := r.Publish(context.Background(), msgAt("k3a", 3, "k"))
	if !res.Accepted || !res.Held {
		t.Fatalf("want held, got %+v", res)
	}
	// exact resend -> duplicate
	if d := r.Publish(context.Background(), msgAt("k3a", 3, "k")); d.Accepted || d.Reason != RejectDuplicate {
		t.Fatalf("want duplicate, got %+v", d)
	}
	// another id at seq 3 -> held conflict
	if cf := r.Publish(context.Background(), msgAt("k3b", 3, "k")); cf.Accepted || cf.Reason != RejectHeldConflict {
		t.Fatalf("want held_conflict, got %+v", cf)
	}
	snap := r.Snapshot()
	if snap.HeldConflicts != 1 || snap.Duplicates < 1 {
		t.Fatalf("counters: %+v", snap)
	}
	t.Logf("topic=k duplicate vs held_conflict distinguished; conflicts=%d duplicates=%d",
		snap.HeldConflicts, snap.Duplicates)
}

// 2a) Gap never fills; capacity bound settles it (continue policy).
func TestReorderCapacitySettlesContinue(t *testing.T) {
	sink := &gapSink{}
	r := testRouter(t, reorderCfg(ReorderPolicy{
		MaxHeld: 3, MaxWait: 10 * time.Second, OnUnfillable: GapContinue,
	}, sink))
	c := newCollector()
	sub, _ := r.Subscribe("c", c.handler, WithBufferSize(128))
	defer sub.Close()

	mustPublish(t, r, msgAt("c1", 1, "c"))
	// park 4,5,6 behind missing 2,3 (3 slots); 7th seq overflows capacity.
	for _, seq := range []uint64{4, 5, 6} {
		if res := r.Publish(context.Background(), msgAt("c"+uitoa(seq), seq, "c")); !res.Held {
			t.Fatalf("seq=%d want held, got %+v", seq, res)
		}
	}
	res := r.Publish(context.Background(), msgAt("c7", 7, "c"))
	// Capacity settles: 2,3 lost; 4,5,6 released in order; then 7 parks again
	// behind... no: frontier jumps to 7, so seq 7 is now in-order and accepted.
	if !res.Accepted {
		t.Fatalf("after capacity-continue, seq7 should be accepted in order, got %+v", res)
	}
	seqs, ids := c.waitN(t, 5, time.Second) // 1,4,5,6,7
	assertStrictIncreasing(t, seqs, ids)
	if seqs[0] != 1 || seqs[1] != 4 || seqs[4] != 7 {
		t.Fatalf("capacity-continue order wrong: %v", seqs)
	}
	var capEvent *GapEvent
	for i := range sink.events {
		if sink.events[i].Basis == GapCapacity {
			capEvent = &sink.events[i]
		}
	}
	if capEvent == nil || len(capEvent.Missing) != 2 || capEvent.Missing[0] != 2 || capEvent.Missing[1] != 3 {
		t.Fatalf("capacity event wrong: %+v", capEvent)
	}
	snap := r.Snapshot()
	if snap.GapsLostCapacity != 1 || snap.MissingSeqs != 2 || snap.HeldNow != 0 {
		t.Fatalf("counters: %+v", snap)
	}
	t.Logf("topic=c capacity settle basis=%s missing=%v released=%v delivered=%v",
		capEvent.Basis, capEvent.Missing, capEvent.Released, seqs)
}

// 2b) Gap never fills; wait bound settles it (terminate policy): stream ends.
func TestReorderTimeoutSettlesTerminate(t *testing.T) {
	sink := &gapSink{}
	r := testRouter(t, reorderCfg(ReorderPolicy{
		MaxHeld: 8, MaxWait: 60 * time.Millisecond, OnUnfillable: GapTerminate,
	}, sink))
	c := newCollector()
	sub, _ := r.Subscribe("e", c.handler, WithBufferSize(128))
	defer sub.Close()

	mustPublish(t, r, msgAt("e1", 1, "e"))
	for _, seq := range []uint64{4, 5} {
		if res := r.Publish(context.Background(), msgAt("e"+uitoa(seq), seq, "e")); !res.Held {
			t.Fatalf("seq=%d want held", seq)
		}
	}
	// Wait for the timeout settlement: 2,3 lost, 4,5 ended, stream terminated.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap := r.Snapshot()
		if snap.StreamsEnded == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if r.Snapshot().StreamsEnded != 1 {
		t.Fatalf("stream did not end on timeout: %+v", r.Snapshot())
	}
	// Nothing above the gap was delivered out of order.
	if seqs, _ := c.snapshot(); len(seqs) != 1 || seqs[0] != 1 {
		t.Fatalf("terminated stream must not release parked msgs, got %v", seqs)
	}
	// Every later publish is rejected with the distinct stream_terminated.
	res := r.Publish(context.Background(), msgAt("e6", 6, "e"))
	if res.Accepted || res.Reason != RejectStreamTerminated {
		t.Fatalf("want stream_terminated, got %+v", res)
	}
	var ev *GapEvent
	for i := range sink.events {
		if sink.events[i].Basis == GapTimeout {
			ev = &sink.events[i]
		}
	}
	if ev == nil || ev.Action != GapTerminate || len(ev.Missing) != 2 || len(ev.Ended) != 2 {
		t.Fatalf("timeout-terminate event wrong: %+v", ev)
	}
	snap := r.Snapshot()
	if snap.GapsLostTimeout != 1 || snap.MissingSeqs != 2 || snap.StreamRejects < 1 {
		t.Fatalf("counters: %+v", snap)
	}
	t.Logf("topic=e timeout settle basis=timeout action=terminate missing=%v ended=%v rejects=%d",
		ev.Missing, ev.Ended, snap.StreamRejects)
}

// 3) Multiple publishers racing on one enabled key: delivered seq strictly
// increasing, no loss within the frontier, no dup; losers get distinct codes.
func TestReorderConcurrentPublishers(t *testing.T) {
	sink := &gapSink{}
	r := testRouter(t, reorderCfg(ReorderPolicy{
		MaxHeld: 256, MaxWait: 5 * time.Second, OnUnfillable: GapContinue,
	}, sink))
	c := newCollector()
	sub, _ := r.Subscribe("hot", c.handler, WithBufferSize(512))
	defer sub.Close()

	const n = 150
	const publishers = 8
	var wg sync.WaitGroup
	var accepted, held, rejected int64
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for seq := uint64(1); seq <= n; seq++ {
				id := "p" + uitoa(uint64(p)) + "-" + uitoa(seq)
				res := r.Publish(context.Background(), msgAt(id, seq, "hot"))
				switch {
				case res.Accepted && res.Held:
					atomic.AddInt64(&held, 1)
				case res.Accepted:
					atomic.AddInt64(&accepted, 1)
				default:
					atomic.AddInt64(&rejected, 1)
				}
			}
		}(p)
	}
	wg.Wait()

	// Every seq 1..n must be delivered exactly once, strictly increasing.
	seqs, ids := c.waitN(t, n, 3*time.Second)
	if len(seqs) != n {
		t.Fatalf("delivered %d, want %d", len(seqs), n)
	}
	assertStrictIncreasing(t, seqs, ids)
	for i, s := range seqs {
		if s != uint64(i+1) {
			t.Fatalf("gap/dup in delivery at %d: %v", i, seqs)
		}
	}
	// All pending gaps must have eventually settled as fills (no timeout).
	time.Sleep(50 * time.Millisecond)
	for _, e := range sink.list() {
		if e.Basis != GapFilled {
			t.Fatalf("concurrent in-window reordering must fill, got %s %+v", e.Basis, e)
		}
	}
	snap := r.Snapshot()
	if snap.HeldNow != 0 {
		t.Fatalf("no messages remain parked: %+v", snap)
	}
	t.Logf("topic=hot concurrent: accepted_now=%d held_then_released=%d losers=%d delivered=1..%d exactly once",
		atomic.LoadInt64(&accepted), snap.ReleasedFromHold, atomic.LoadInt64(&rejected), n)
}

// A key without a policy behaves exactly as before: immediate seq_gap.
func TestReorderDisabledKeyUnchanged(t *testing.T) {
	sink := &gapSink{}
	r := testRouter(t, Config{
		Reorder: func(topic, key string) (ReorderPolicy, bool) {
			if key == "buffered" {
				return ReorderPolicy{MaxHeld: 4, MaxWait: time.Second}, true
			}
			return ReorderPolicy{}, false
		},
		OnGapEvent: sink.add,
	})
	c := newCollector()
	sub, _ := r.Subscribe("strict", c.handler, WithBufferSize(64))
	defer sub.Close()
	mustPublish(t, r, msgAt("s1", 1, "strict"))
	assertReason(t, r, msgAt("s3", 3, "strict"), RejectGap)
	if len(sink.list()) != 0 {
		t.Fatalf("disabled key must not emit gap events")
	}
	if snap := r.Snapshot(); snap.HeldTotal != 0 {
		t.Fatalf("disabled key must not park, held=%d", snap.HeldTotal)
	}
	t.Logf("topic=strict reorder disabled: gap rejected immediately, semantics unchanged")
}

// 4) Restart with held (unfilled) messages: they remain parked after recovery
// and are released in order once the gap fills — never re-delivered, never lost.
func TestReorderRestartHeldThenFills(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "held.log")

	open := func() (*Router, *Journal) {
		j, err := OpenJournal(path)
		if err != nil {
			t.Fatal(err)
		}
		r := testRouter(t, Config{
			Journal:      j,
			RetainPerKey: 128,
			Reorder:      enableReorder(ReorderPolicy{MaxHeld: 16, MaxWait: time.Hour, OnUnfillable: GapContinue}),
			OnGapEvent:   func(GapEvent) {},
		})
		return r, j
	}

	r1, _ := open()
	c1 := newCollector()
	sub1, _ := r1.Subscribe("r", c1.handler, WithBufferSize(128))
	mustPublish(t, r1, msgAt("r1", 1, "r"))
	for _, seq := range []uint64{4, 5} {
		if res := r1.Publish(context.Background(), msgAt("r"+uitoa(seq), seq, "r")); !res.Held {
			t.Fatalf("seq=%d want held before restart", seq)
		}
	}
	time.Sleep(30 * time.Millisecond)
	if seqs, _ := c1.snapshot(); len(seqs) != 1 || seqs[0] != 1 {
		t.Fatalf("only seq1 delivered before crash, got %v", seqs)
	}
	sub1.Close()
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: held 4,5 must be restored as parked, not treated as new.
	r2, j2 := open()
	defer j2.Close()
	c2 := newCollector()
	// Durable subscriber with a fresh id gets the retained, already-accepted
	// history (seq 1 plus the later releases) replayed in order — this is the
	// supported way to observe continuity across a restart.
	sub2, _ := r2.Subscribe("r", c2.handler, WithBufferSize(128), WithDurable("r-worker"))
	defer sub2.Close()
	// A duplicate resend of parked id is still suppressed after restart.
	if d := r2.Publish(context.Background(), msgAt("r4", 4, "r")); d.Accepted || d.Reason != RejectDuplicate {
		t.Fatalf("parked id duplicate after restart, got %+v", d)
	}
	// Fill the missing seqs after restart.
	mustPublish(t, r2, msgAt("r2", 2, "r"))
	mustPublish(t, r2, msgAt("r3", 3, "r"))
	seqs, ids := c2.waitN(t, 5, time.Second)
	assertStrictIncreasing(t, seqs, ids)
	if seqs[0] != 1 || seqs[4] != 5 {
		t.Fatalf("after restart want 1..5 once each, got %v", seqs)
	}
	// seq 6 continues the stream.
	mustPublish(t, r2, msgAt("r6", 6, "r"))
	seqs, ids = c2.waitN(t, 6, time.Second)
	assertStrictIncreasing(t, seqs, ids)
	t.Logf("topic=r restart recovery: parked 4,5 restored, delivered=%v after fill", seqs)
}

// Restart after a capacity/timeout settlement: the lost prefix and terminated
// frontier are recovered; a re-enabled stream does not silently resurrect.
func TestReorderRestartTerminatedStaysTerminated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "z.log")
	policy := ReorderPolicy{MaxHeld: 2, MaxWait: time.Hour, OnUnfillable: GapTerminate}

	j1, _ := OpenJournal(path)
	r1 := testRouter(t, Config{Journal: j1, RetainPerKey: 64, Reorder: enableReorder(policy)})
	mustPublish(t, r1, msgAt("z1", 1, "z"))
	for _, seq := range []uint64{3, 4} {
		if res := r1.Publish(context.Background(), msgAt("z"+uitoa(seq), seq, "z")); !res.Held {
			t.Fatalf("seq=%d want held", seq)
		}
	}
	// Third overflow -> capacity settlement -> terminate.
	res := r1.Publish(context.Background(), msgAt("z5", 5, "z"))
	if res.Accepted || res.Reason != RejectStreamTerminated {
		t.Fatalf("want terminated, got %+v", res)
	}
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}

	j2, _ := OpenJournal(path)
	r2, err := New(Config{Journal: j2, RetainPerKey: 64, Reorder: enableReorder(policy), Logger: testLogger{t}})
	if err != nil {
		t.Fatalf("recovery should succeed: %v", err)
	}
	t.Cleanup(func() { _ = r2.Close() })
	if d := r2.Publish(context.Background(), msgAt("z6", 6, "z")); d.Accepted || d.Reason != RejectStreamTerminated {
		t.Fatalf("terminated state must survive restart, got %+v", d)
	}
	t.Logf("topic=z terminated frontier recovered after restart; later publishes remain rejected")
}

// If reorder state exists in the journal but the selector no longer enables the
// key, recovery must fail loudly rather than silently switch semantics.
func TestReorderRestartPolicyRemovedFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.log")
	j1, _ := OpenJournal(path)
	r1 := testRouter(t, Config{
		Journal: j1, RetainPerKey: 64,
		Reorder: enableReorder(ReorderPolicy{MaxHeld: 8, MaxWait: time.Hour}),
	})
	mustPublish(t, r1, msgAt("p1", 1, "p"))
	if res := r1.Publish(context.Background(), msgAt("p3", 3, "p")); !res.Held {
		t.Fatalf("want held: %+v", res)
	}
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}

	j2, _ := OpenJournal(path)
	_, err := New(Config{
		Journal: j2, Logger: testLogger{t},
		Reorder: func(topic, key string) (ReorderPolicy, bool) { return ReorderPolicy{}, false },
	})
	if !errors.Is(err, ErrJournalCorrupt) {
		t.Fatalf("want explicit recovery error (ErrJournalCorrupt), got %v", err)
	}
	t.Logf("policy removed at restart: New() refuses with %v (no silent semantic change)", err)
}

// A torn/unknown/garbage journal frame still fails startup (regression of the
// existing guarantee) with the new record kinds present.
func TestReorderJournalCorruptionStillFatal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt-reorder.log")
	j, _ := OpenJournal(path)
	r := testRouter(t, Config{Journal: j, Reorder: enableReorder(
		ReorderPolicy{MaxHeld: 4, MaxWait: time.Second})})
	mustPublish(t, r, msgAt("x1", 1, "x"))
	if res := r.Publish(context.Background(), msgAt("x3", 3, "x")); !res.Held {
		t.Fatalf("want held: %+v", res)
	}
	_ = r.Close()

	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	j2, _ := OpenJournal(path)
	_, err := New(Config{Journal: j2, Reorder: enableReorder(
		ReorderPolicy{MaxHeld: 4, MaxWait: time.Second}), Logger: testLogger{t}})
	if !errors.Is(err, ErrJournalCorrupt) {
		t.Fatalf("want ErrJournalCorrupt, got %v", err)
	}
	t.Logf("torn hold frame still aborts startup with %v", ErrJournalCorrupt)
}

// Slow subscriber under the reorder path still cannot block the publisher;
// released batches are dropped non-blockingly and counted.
func TestReorderSlowSubscriberNeverBlocks(t *testing.T) {
	var dropped int64
	r := testRouter(t, reorderCfg(ReorderPolicy{
		MaxHeld: 64, MaxWait: time.Second, OnUnfillable: GapContinue,
	}, nil))
	gate := make(chan struct{})
	slow, err := r.Subscribe("slow", func(_ context.Context, _ *Message) error {
		<-gate
		return nil
	}, WithBufferSize(2), WithOverflowCallback(func(OverflowEvent) {
		atomic.AddInt64(&dropped, 1)
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	fast, err := r.Subscribe("slow", func(_ context.Context, _ *Message) error { return nil },
		WithBufferSize(256))
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close()

	var pubErr atomic.Value
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		// Publish 1 then 3..40 (held), then fill 2 -> batch release.
		for _, m := range []*Message{msgAt("s1", 1, "slow")} {
			if res := r.Publish(context.Background(), m); !res.Accepted {
				pubErr.Store(res.Detail)
				return
			}
		}
		for seq := uint64(3); seq <= 40; seq++ {
			r.Publish(context.Background(), msgAt("s"+uitoa(seq), seq, "slow"))
		}
		if res := r.Publish(context.Background(), msgAt("s2", 2, "slow")); !res.Accepted {
			pubErr.Store(res.Detail)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publisher blocked by slow subscriber during batch release")
	}
	if e := pubErr.Load(); e != nil {
		t.Fatalf("publish during batch release: %v", e)
	}
	close(gate)
	t.Logf("reorder batch of 40 offered in %v despite blocked slow subscriber; backpressure drops=%d",
		time.Since(start), atomic.LoadInt64(&dropped))
}

// Deeper gap behind a released run: capacity settlement releases the contiguous
// parked run above the lost prefix, keeps later parked messages waiting, and the
// stream then continues correctly once the deeper gap also resolves (by timeout).
func TestReorderContinueWithDeeperGap(t *testing.T) {
	sink := &gapSink{}
	r := testRouter(t, reorderCfg(ReorderPolicy{
		MaxHeld: 2, MaxWait: 80 * time.Millisecond, OnUnfillable: GapContinue,
	}, sink))
	c := newCollector()
	sub, _ := r.Subscribe("d", c.handler, WithBufferSize(128))
	defer sub.Close()

	mustPublish(t, r, msgAt("d1", 1, "d"))
	// park 3 (missing 2) and 4 (contiguous with 3); capacity 2 -> seq 6
	// triggers settlement: missing 2, release 3,4, deeper gap at 5 keeps 6 parked.
	for _, seq := range []uint64{3, 4} {
		if res := r.Publish(context.Background(), msgAt("d"+uitoa(seq), seq, "d")); !res.Held {
			t.Fatalf("seq=%d want held", seq)
		}
	}
	res := r.Publish(context.Background(), msgAt("d6", 6, "d"))
	if !res.Held {
		t.Fatalf("seq6 must remain parked behind deeper gap 5, got %+v", res)
	}
	seqs, _ := c.waitN(t, 3, time.Second) // 1,3,4
	if seqs[0] != 1 || seqs[1] != 3 || seqs[2] != 4 {
		t.Fatalf("after first settle want [1 3 4], got %v", seqs)
	}
	// Second timeout: 5 lost, 6 released; frontier advances to 7.
	waitUntil(t, 2*time.Second, func() bool {
		return r.Snapshot().GapsLostTimeout == 1
	})
	seqs, _ = c.snapshot()
	if len(seqs) != 4 || seqs[3] != 6 {
		t.Fatalf("after second settle want 6 released, got %v", seqs)
	}
	mustPublish(t, r, msgAt("d7", 7, "d"))
	seqs, _ = c.waitN(t, 5, time.Second)
	if len(seqs) != 5 || seqs[4] != 7 {
		t.Fatalf("stream should continue at 7, got %v", seqs)
	}
	// Two distinct unfillable conclusions, each with its missing seq.
	var missingTotal int
	for _, e := range sink.list() {
		if e.Basis == GapCapacity {
			if len(e.Missing) != 1 || e.Missing[0] != 2 || len(e.Released) != 2 {
				t.Fatalf("capacity event wrong: %+v", e)
			}
		}
		if e.Basis == GapTimeout {
			if len(e.Missing) != 1 || e.Missing[0] != 5 || len(e.Released) != 1 || e.Released[0] != 6 {
				t.Fatalf("timeout event wrong: %+v", e)
			}
		}
		missingTotal += len(e.Missing)
	}
	if missingTotal != 2 {
		t.Fatalf("want exactly 2 missing seqs reported, events=%+v", sink.list())
	}
	t.Logf("topic=d deeper gap handled: delivered=%v (missing 2 and 5 each reported)", seqs)
}

// A Continue settlement with a deeper still-parked gap must round-trip across
// restart: released seqs stay accepted (no redelivery into the frontier), the
// deeper parked messages remain held, and the next fill joins in order.
func TestReorderRestartContinueWithDeeperGap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "deep.log")
	policy := ReorderPolicy{MaxHeld: 2, MaxWait: time.Hour, OnUnfillable: GapContinue}

	j1, _ := OpenJournal(path)
	r1 := testRouter(t, Config{Journal: j1, RetainPerKey: 128, Reorder: enableReorder(policy)})
	mustPublish(t, r1, msgAt("g1", 1, "g"))
	for _, seq := range []uint64{3, 4} {
		if res := r1.Publish(context.Background(), msgAt("g"+uitoa(seq), seq, "g")); !res.Held {
			t.Fatalf("seq=%d want held", seq)
		}
	}
	// Capacity settles: 2 lost, 3,4 released; seq 6 arrives and parks behind
	// the deeper gap at 5.
	if res := r1.Publish(context.Background(), msgAt("g6", 6, "g")); !res.Held {
		t.Fatalf("seq6 want held behind gap5, got %+v", res)
	}
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}

	j2, _ := OpenJournal(path)
	r2, err := New(Config{Journal: j2, RetainPerKey: 128, Reorder: enableReorder(policy), Logger: testLogger{t}})
	if err != nil {
		t.Fatalf("recover deeper-gap continue: %v", err)
	}
	t.Cleanup(func() { _ = r2.Close() })
	sh := r2.shardFor(topicKey{topic: "g", key: "g"})
	sh.mu.Lock()
	st := sh.state[topicKey{topic: "g", key: "g"}]
	nHeld := len(st.held)
	_, sixHeld := st.held[6]
	frontier := st.nextSeq
	sh.mu.Unlock()
	if nHeld != 1 || !sixHeld || frontier != 5 {
		t.Fatalf("after restart want exactly seq6 held and frontier=5, got held=%d six=%v nextSeq=%d",
			nHeld, sixHeld, frontier)
	}
	// A replay of already-released seq 3 is a rollback (accepted history), not
	// a re-acceptance.
	if d := r2.Publish(context.Background(), msgAt("g3", 3, "g")); d.Accepted || d.Reason != RejectDuplicate {
		t.Fatalf("released seq3 must remain duplicate after restart, got %+v", d)
	}
	// Filling seq 5 releases 6 in order; stream continues.
	c := newCollector()
	sub, _ := r2.Subscribe("g", c.handler, WithBufferSize(128), WithDurable("g-worker"))
	defer sub.Close()
	mustPublish(t, r2, msgAt("g5", 5, "g"))
	// Durable view: replay of accepted history 1,3,4 (seq2 was declared lost,
	// never committed), then live 5,6 — strictly increasing, no dup.
	seqs, ids := c.waitN(t, 5, time.Second)
	assertStrictIncreasing(t, seqs, ids)
	want := []uint64{1, 3, 4, 5, 6}
	if len(seqs) != len(want) {
		t.Fatalf("want %v, got %v", want, seqs)
	}
	for i := range want {
		if seqs[i] != want[i] {
			t.Fatalf("want %v, got %v", want, seqs)
		}
	}
	t.Logf("topic=g deeper-gap continue survived restart; durable view delivered=%v", seqs)
}
