package router

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// oooAll enables the window for every key with test-friendly bounds.
func oooAll(capacity int, maxWait time.Duration) *OutOfOrderConfig {
	return &OutOfOrderConfig{
		Match:         func(string, string) bool { return true },
		Capacity:      capacity,
		MaxWait:       maxWait,
		SweepInterval: 10 * time.Millisecond,
	}
}

// assertPending expects the message to be held by the out-of-order window.
func assertPending(t *testing.T, r *Router, m *Message) PublishResult {
	t.Helper()
	res := r.Publish(t.Context(), m)
	if res.Accepted || !res.Pending || res.Reason != RejectGapPending {
		t.Fatalf("topic=%s seq=%d id=%s: want gap_pending, got accepted=%v pending=%v reason=%s detail=%s",
			m.Topic, m.Seq, m.ID, res.Accepted, res.Pending, res.Reason, res.Detail)
	}
	t.Logf("held topic=%s seq=%d id=%s basis=%s", m.Topic, m.Seq, m.ID, res.Detail)
	return res
}

// seqCollector collects delivered seqs of one subscriber.
type seqCollector struct {
	mu   sync.Mutex
	seqs []uint64
}

func (c *seqCollector) handler() HandlerFunc {
	return func(_ context.Context, m *Message) error {
		c.mu.Lock()
		c.seqs = append(c.seqs, m.Seq)
		c.mu.Unlock()
		return nil
	}
}

func (c *seqCollector) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seqs)
}

// requireStrictlyIncreasing asserts the collected seqs equal want exactly.
func (c *seqCollector) requireSeqs(t *testing.T, want ...uint64) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.seqs) != len(want) {
		t.Fatalf("delivered seqs=%v, want %v", c.seqs, want)
	}
	for i, v := range want {
		if c.seqs[i] != v {
			t.Fatalf("delivered seqs=%v, want %v (first diff at %d)", c.seqs, want, i)
		}
	}
	t.Logf("delivered seqs=%v strictly increasing, no loss, no dup", c.seqs)
}

// A gap that is later filled: held messages are released in original order.
func TestOutOfOrderGapFilledLater(t *testing.T) {
	r := testRouter(t, Config{OutOfOrder: oooAll(8, 5*time.Second)})
	c := &seqCollector{}
	sub, err := r.Subscribe("win", c.handler(), WithBufferSize(16))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	mustPublish(t, r, msgAt("w1", 1, "win"))
	assertPending(t, r, msgAt("w3", 3, "win"))
	assertPending(t, r, msgAt("w4", 4, "win"))

	snap := r.Snapshot()
	if snap.GapPending != 2 || snap.PendingNow != 2 {
		t.Fatalf("pending counters wrong: %+v", snap)
	}

	// The missing seq arrives: 2 accepted, 3 and 4 released in order.
	mustPublish(t, r, msgAt("w2", 2, "win"))
	waitUntil(t, 2*time.Second, func() bool { return c.len() == 4 })
	c.requireSeqs(t, 1, 2, 3, 4)

	snap = r.Snapshot()
	if snap.PendingReleased != 2 || snap.PendingNow != 0 {
		t.Fatalf("release counters wrong: %+v", snap)
	}
	t.Logf("gap filled: released=%d pending_now=%d", snap.PendingReleased, snap.PendingNow)
}

// Keys not matched by the window keep the legacy seq_gap behavior exactly.
func TestOutOfOrderDisabledKeyUnchanged(t *testing.T) {
	cfg := &OutOfOrderConfig{
		Match:         func(topic, key string) bool { return topic == "win" },
		Capacity:      4,
		MaxWait:       time.Second,
		SweepInterval: 10 * time.Millisecond,
	}
	r := testRouter(t, Config{OutOfOrder: cfg})

	mustPublish(t, r, msgAt("p1", 1, "plain"))
	// Legacy behavior: immediate, distinguishable seq_gap rejection.
	assertReason(t, r, msgAt("p3", 3, "plain"), RejectGap)
	// The gap does not advance the frontier: seq=2 still accepted.
	mustPublish(t, r, msgAt("p2", 2, "plain"))

	// Matched key holds instead of rejecting.
	mustPublish(t, r, msgAt("w1", 1, "win"))
	assertPending(t, r, msgAt("w3", 3, "win"))

	snap := r.Snapshot()
	if snap.SeqGaps != 1 || snap.GapPending != 1 {
		t.Fatalf("disabled/enabled keys not distinguishable: %+v", snap)
	}
	t.Logf("disabled key: seq_gap reject unchanged; enabled key: held (gap_pending=%d)", snap.GapPending)
}

// Capacity bound reached: the gap is settled (skip), the lost seq is ruled
// unrecoverable, held messages are released in order, and the latecomer for
// the lost seq is explicitly rejected as a rollback.
func TestOutOfOrderCapacitySettlementSkip(t *testing.T) {
	cfg := oooAll(2, 5*time.Second)
	r := testRouter(t, Config{OutOfOrder: cfg})
	c := &seqCollector{}
	sub, err := r.Subscribe("cap", c.handler(), WithBufferSize(16))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	mustPublish(t, r, msgAt("c1", 1, "cap"))
	assertPending(t, r, msgAt("c3", 3, "cap"))
	assertPending(t, r, msgAt("c4", 4, "cap"))

	// A third out-of-order message would exceed Capacity=2: the gap [2..2]
	// is settled as lost, 3 and 4 are released, 6 is held.
	assertPending(t, r, msgAt("c6", 6, "cap"))
	snap := r.Snapshot()
	if snap.GapSettledCapacity != 1 || snap.SeqsDeclaredLost != 1 {
		t.Fatalf("capacity settlement not distinguishable: %+v", snap)
	}

	// The seq ruled lost can never be accepted again.
	assertReason(t, r, msgAt("c2", 2, "cap"), RejectRollback)

	// The stream continues in order after the settlement.
	mustPublish(t, r, msgAt("c5", 5, "cap"))
	waitUntil(t, 2*time.Second, func() bool { return c.len() == 5 })
	c.requireSeqs(t, 1, 3, 4, 5, 6)

	snap = r.Snapshot()
	if snap.PendingReleased != 3 || snap.PendingNow != 0 {
		t.Fatalf("release counters wrong: %+v", snap)
	}
	t.Logf("capacity settlement: lost=[2..2] cause=capacity action=skip; delivered in order; late seq=2 -> seq_rollback")
}

// Wait budget exhausted: the sweeper settles the gap (timeout), releases the
// held message and the stream continues in order.
func TestOutOfOrderTimeoutSettlementSkip(t *testing.T) {
	cfg := oooAll(8, 60*time.Millisecond)
	r := testRouter(t, Config{OutOfOrder: cfg})
	c := &seqCollector{}
	sub, err := r.Subscribe("tmo", c.handler(), WithBufferSize(16))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	mustPublish(t, r, msgAt("t1", 1, "tmo"))
	assertPending(t, r, msgAt("t3", 3, "tmo"))

	// seq=2 never arrives: after MaxWait the gap is settled by the sweeper.
	waitUntil(t, 2*time.Second, func() bool {
		return r.Snapshot().GapSettledTimeout == 1
	})
	waitUntil(t, 2*time.Second, func() bool { return c.len() == 2 })

	// The lost seq stays lost; the stream goes on.
	assertReason(t, r, msgAt("t2", 2, "tmo"), RejectRollback)
	mustPublish(t, r, msgAt("t4", 4, "tmo"))
	waitUntil(t, 2*time.Second, func() bool { return c.len() == 3 })
	c.requireSeqs(t, 1, 3, 4)

	snap := r.Snapshot()
	if snap.GapSettledTimeout != 1 || snap.SeqsDeclaredLost != 1 || snap.GapSettledCapacity != 0 {
		t.Fatalf("timeout settlement not distinguishable: %+v", snap)
	}
	t.Logf("timeout settlement: lost=[2..2] cause=timeout action=skip; delivered 1,3,4 strictly increasing")
}

// GapActionTerminate: settlement ends the stream; held messages are dropped
// explicitly and every later publish is rejected stream_terminated.
func TestOutOfOrderTerminate(t *testing.T) {
	cfg := oooAll(2, 5*time.Second)
	cfg.OnSettle = GapActionTerminate
	r := testRouter(t, Config{OutOfOrder: cfg})
	c := &seqCollector{}
	sub, err := r.Subscribe("term", c.handler(), WithBufferSize(16))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	mustPublish(t, r, msgAt("x1", 1, "term"))
	assertPending(t, r, msgAt("x3", 3, "term"))
	assertPending(t, r, msgAt("x4", 4, "term"))

	// Capacity exceeded -> settlement terminates the stream; the triggering
	// message itself is rejected with the distinguishable reason.
	assertReason(t, r, msgAt("x5", 5, "term"), RejectStreamTerminated)
	// So is everything after, even the seq that was originally missing.
	assertReason(t, r, msgAt("x2", 2, "term"), RejectStreamTerminated)
	assertReason(t, r, msgAt("x6", 6, "term"), RejectStreamTerminated)

	time.Sleep(50 * time.Millisecond)
	c.requireSeqs(t, 1)

	snap := r.Snapshot()
	if snap.TerminatedKeys != 1 || snap.TerminatedPendingDrops != 2 || snap.StreamTerminated != 3 {
		t.Fatalf("termination not distinguishable: %+v", snap)
	}
	t.Logf("terminate: held seq=3,4 dropped (counted), stream_terminated=%d, delivered only seq=1",
		snap.StreamTerminated)
}

// Many publishers racing on the same key produce arbitrary arrival order;
// the window reorders them so delivery is exactly seq 1..N, once each.
func TestOutOfOrderConcurrentPublishers(t *testing.T) {
	cfg := oooAll(512, 5*time.Second)
	r := testRouter(t, Config{OutOfOrder: cfg})
	c := &seqCollector{}
	sub, err := r.Subscribe("hot", c.handler(), WithBufferSize(256))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	const n = 200
	const producers = 4
	var wg sync.WaitGroup
	var rejected int64
	var mu sync.Mutex
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			lo := uint64(p*(n/producers)) + 1
			hi := uint64((p + 1) * (n / producers))
			for seq := lo; seq <= hi; seq++ {
				res := r.Publish(context.Background(),
					msgAt(fmt.Sprintf("p%d-%d", p, seq), seq, "hot"))
				if !res.Accepted && !res.Pending {
					mu.Lock()
					rejected++
					mu.Unlock()
					t.Errorf("seq=%d unexpectedly concluded: %s", seq, res)
				}
			}
		}(p)
	}
	wg.Wait()
	if rejected != 0 {
		t.Fatalf("unexpected rejections: %d", rejected)
	}

	waitUntil(t, 5*time.Second, func() bool { return c.len() == n })
	c.mu.Lock()
	for i, v := range c.seqs {
		if v != uint64(i+1) {
			c.mu.Unlock()
			t.Fatalf("order broken at %d: got seq=%d", i, v)
		}
	}
	c.mu.Unlock()

	snap := r.Snapshot()
	if snap.Accepted+snap.PendingReleased != n || snap.Rejected != 0 {
		t.Fatalf("accepted(%d)+released(%d) != %d or rejects(%d) != 0",
			snap.Accepted, snap.PendingReleased, n, snap.Rejected)
	}
	t.Logf("concurrent publishers: delivered 1..%d strictly increasing; immediate=%d held_then_released=%d rejected=%d",
		n, snap.Accepted, snap.PendingReleased, snap.Rejected)
}

// Restart with held messages: the window is rebuilt from the journal, the
// late gap-filler releases exactly the held messages in order, and nothing
// is delivered twice or reordered.
func TestOutOfOrderRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ooo.log")

	// Lifetime 1: seq 1 delivered; seq 3,4 held (journaled as pending).
	j1, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	r1 := testRouter(t, Config{Journal: j1, RetainPerKey: 100, OutOfOrder: oooAll(8, 5*time.Second)})
	c1 := &seqCollector{}
	s1, err := r1.Subscribe("evt", c1.handler(), WithBufferSize(16))
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, r1, msgAt("e1", 1, "evt"))
	assertPending(t, r1, msgAt("e3", 3, "evt"))
	assertPending(t, r1, msgAt("e4", 4, "evt"))
	waitUntil(t, 2*time.Second, func() bool { return c1.len() == 1 })
	s1.Close()
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}

	// Lifetime 2: held messages must be recovered, not forgotten.
	j2, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	r2 := testRouter(t, Config{Journal: j2, RetainPerKey: 100, OutOfOrder: oooAll(8, 5*time.Second)})
	if got := r2.Snapshot().PendingNow; got != 2 {
		t.Fatalf("pending window not recovered: pending_now=%d want 2", got)
	}
	c2 := &seqCollector{}
	s2, err := r2.Subscribe("evt", c2.handler(), WithBufferSize(16))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// The gap fills after the restart: 2 accepted, 3 and 4 released in order.
	mustPublish(t, r2, msgAt("e2", 2, "evt"))
	mustPublish(t, r2, msgAt("e5", 5, "evt"))
	waitUntil(t, 2*time.Second, func() bool { return c2.len() == 4 })
	c2.requireSeqs(t, 2, 3, 4, 5)
	c1.requireSeqs(t, 1)

	// Already-delivered seq=1 is still a duplicate, not a redelivery.
	assertReason(t, r2, msgAt("e1", 1, "evt"), RejectDuplicate)
	t.Logf("restart: pending=2 recovered; delivered lifetime1=[1] lifetime2=[2,3,4,5]; no loss, no dup, order preserved")
}

// A settlement verdict survives the restart: seqs ruled lost stay lost.
func TestOutOfOrderRestartAfterSettlement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "settle.log")

	j1, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	r1 := testRouter(t, Config{Journal: j1, OutOfOrder: oooAll(8, 50*time.Millisecond)})
	mustPublish(t, r1, msgAt("s1", 1, "settled"))
	assertPending(t, r1, msgAt("s3", 3, "settled"))
	waitUntil(t, 2*time.Second, func() bool {
		return r1.Snapshot().GapSettledTimeout == 1
	})
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}

	j2, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	r2 := testRouter(t, Config{Journal: j2, OutOfOrder: oooAll(8, 50*time.Millisecond)})
	// seq=2 was ruled lost before the restart: still rejected.
	assertReason(t, r2, msgAt("s2", 2, "settled"), RejectRollback)
	// The stream continues where the settlement left it.
	mustPublish(t, r2, msgAt("s4", 4, "settled"))
	t.Logf("restart after settlement: lost seq=2 still rejected (seq_rollback), seq=4 accepted")
}

// A terminated stream stays terminated across a restart.
func TestOutOfOrderTerminateRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "term.log")

	cfg := oooAll(1, 5*time.Second)
	cfg.OnSettle = GapActionTerminate

	j1, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	r1 := testRouter(t, Config{Journal: j1, OutOfOrder: cfg})
	mustPublish(t, r1, msgAt("k1", 1, "dead"))
	assertPending(t, r1, msgAt("k3", 3, "dead"))
	assertReason(t, r1, msgAt("k4", 4, "dead"), RejectStreamTerminated)
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}

	j2, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	r2 := testRouter(t, Config{Journal: j2, OutOfOrder: cfg})
	assertReason(t, r2, msgAt("k2", 2, "dead"), RejectStreamTerminated)
	assertReason(t, r2, msgAt("k5", 5, "dead"), RejectStreamTerminated)
	t.Logf("restart after terminate: stream still terminated, publishes rejected stream_terminated")
}

// A journal holding pending records but a config without the window is an
// explicit startup error, never a silent semantic downgrade.
func TestOutOfOrderPendingRequiresWindowAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "downgrade.log")

	j1, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	r1 := testRouter(t, Config{Journal: j1, OutOfOrder: oooAll(8, 5*time.Second)})
	mustPublish(t, r1, msgAt("d1", 1, "guard"))
	assertPending(t, r1, msgAt("d3", 3, "guard"))
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}

	j2, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(Config{Journal: j2, Logger: testLogger{t}})
	if err == nil || !strings.Contains(err.Error(), "out-of-order window is not enabled") {
		t.Fatalf("want explicit recovery error, got %v", err)
	}
	t.Logf("pending records without window config -> explicit error: %v", err)
}
