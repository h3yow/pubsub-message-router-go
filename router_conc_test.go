package router

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Many publishers on the SAME key. Because gaps/rollbacks are rejected, and
// because only one seq==frontier message exists, the total set of accepted
// messages must be exactly seq 1..N with no duplicates, regardless of
// goroutine scheduling. Losing producers receive explicit
// seq_gap/seq_rollback/duplicate conclusions and can retry.
func TestConcurrentPublishSameKeyNoLossNoDup(t *testing.T) {
	r := testRouter(t, Config{})

	const n = 200
	var accepted, dup, gap, rollback int64

	var wg sync.WaitGroup
	for p := 0; p < 8; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for seq := uint64(1); seq <= n; seq++ {
				id := "p" + uitoa(uint64(p)) + "-" + uitoa(seq)
				res := r.Publish(context.Background(), msgAt(id, seq, "hot"))
				switch {
				case res.Accepted:
					atomic.AddInt64(&accepted, 1)
				case res.Reason == RejectDuplicate:
					atomic.AddInt64(&dup, 1)
				case res.Reason == RejectGap:
					atomic.AddInt64(&gap, 1)
				case res.Reason == RejectRollback:
					atomic.AddInt64(&rollback, 1)
				}
			}
		}(p)
	}
	wg.Wait()

	if atomic.LoadInt64(&accepted) != n {
		t.Fatalf("accepted=%d want exactly %d (gap=%d rollback=%d dup=%d)",
			accepted, n, gap, rollback, dup)
	}

	sh := r.shardFor(topicKey{topic: "hot", key: "hot"})
	sh.mu.Lock()
	st := sh.state[topicKey{topic: "hot", key: "hot"}]
	sh.mu.Unlock()
	if st == nil || st.nextSeq != n+1 {
		t.Fatalf("frontier wrong: %+v, want nextSeq=%d", st, n+1)
	}
	seen := make(map[uint64]string, n)
	for mid, seq := range st.recent {
		if other, ok := seen[seq]; ok {
			t.Fatalf("seq %d accepted twice: %s and %s", seq, other, mid)
		}
		seen[seq] = mid
	}
	if len(seen) != n {
		t.Fatalf("unique accepted seqs in window=%d want %d", len(seen), n)
	}
	t.Logf("topic=hot concurrent verdict: accepted=%d (seq 1..%d exactly once); losers gap=%d rollback=%d dup=%d",
		accepted, n, atomic.LoadInt64(&gap), atomic.LoadInt64(&rollback), atomic.LoadInt64(&dup))
}

// Slow subscriber: tiny buffer + blocking handler. The publisher must never
// block; drops are explicit and observable; a second fast subscriber still
// receives everything.
func TestSlowSubscriberBackpressure(t *testing.T) {
	r := testRouter(t, Config{})

	var (
		mu       sync.Mutex
		events   []OverflowEvent
		fastGot  atomic.Int64
		slowGate = make(chan struct{})
	)
	slow, err := r.Subscribe("t",
		func(_ context.Context, _ *Message) error {
			<-slowGate // intentionally blocked until released
			return nil
		},
		WithBufferSize(2),
		WithOverflowPolicy(OverflowDropNewest),
		WithOverflowCallback(func(e OverflowEvent) {
			mu.Lock()
			events = append(events, e)
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	fast, err := r.Subscribe("t",
		func(_ context.Context, _ *Message) error {
			fastGot.Add(1)
			return nil
		}, WithBufferSize(128))
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	defer fast.Close()

	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		for seq := uint64(1); seq <= 40; seq++ {
			res := r.Publish(context.Background(), msgAt("b"+uitoa(seq), seq, "t"))
			if !res.Accepted {
				t.Errorf("publish seq=%d unexpectedly rejected: %s", seq, res.Reason)
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publisher blocked by slow subscriber")
	}
	t.Logf("publisher offered 40 messages in %v despite slow subscriber", time.Since(start))

	close(slowGate)

	waitUntil(t, time.Second, func() bool { return fastGot.Load() == 40 })
	waitUntil(t, time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(events) > 0
	})
	mu.Lock()
	nEvents := len(events)
	sample := events[0]
	mu.Unlock()
	snap := r.Snapshot()
	if nEvents == 0 || snap.DroppedBackpressure == 0 {
		t.Fatalf("backpressure not observable: events=%d counter=%d", nEvents, snap.DroppedBackpressure)
	}
	t.Logf("slow subscriber overflow: events=%d counter=%d sample={subscriber=%s topic=%s seq=%d policy=%s droppedID=%s}; fast got=%d/40",
		nEvents, snap.DroppedBackpressure, sample.SubscriberID, sample.Topic,
		sample.Seq, sample.Policy, sample.DroppedID, fastGot.Load())
}

// OverflowDropOldest must keep the newest message and drop from the head.
func TestDropOldestKeepsNewest(t *testing.T) {
	r := testRouter(t, Config{})

	var mu sync.Mutex
	var got []uint64
	gate := make(chan struct{})
	var dropped atomic.Int64
	sub, err := r.Subscribe("t", func(_ context.Context, m *Message) error {
		<-gate
		mu.Lock()
		got = append(got, m.Seq)
		mu.Unlock()
		return nil
	},
		WithBufferSize(2),
		WithOverflowPolicy(OverflowDropOldest),
		WithOverflowCallback(func(OverflowEvent) { dropped.Add(1) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	for seq := uint64(1); seq <= 10; seq++ {
		r.Publish(context.Background(), msgAt("d"+uitoa(seq), seq, "t"))
	}
	close(gate)
	waitUntil(t, time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) >= 1
	})
	time.Sleep(30 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 {
		t.Fatal("nothing delivered")
	}
	if got[len(got)-1] != 10 {
		t.Fatalf("drop_oldest must keep newest; last=%d order=%v", got[len(got)-1], got)
	}
	if dropped.Load() == 0 {
		t.Fatal("expected overflow drops to be counted")
	}
	t.Logf("drop_oldest delivered seqs=%v (newest=10 present), drops=%d", got, dropped.Load())
}
