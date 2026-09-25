package router

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestRoutingWildcardsAndFanout(t *testing.T) {
	r := testRouter(t, Config{})
	var mu sync.Mutex
	got := map[string][]uint64{}
	recv := func(name string) HandlerFunc {
		return func(_ context.Context, m *Message) error {
			mu.Lock()
			got[name] = append(got[name], m.Seq)
			mu.Unlock()
			return nil
		}
	}
	s1, _ := r.Subscribe("a/+", recv("plus"))
	s2, _ := r.Subscribe("a/#", recv("hash"))
	s3, _ := r.Subscribe("b", recv("b"))
	defer s1.Close()
	defer s2.Close()
	defer s3.Close()

	for seq := uint64(1); seq <= 3; seq++ {
		mustPublish(t, r, msgAt(id(seq), seq, "a/x"))
	}
	mustPublish(t, r, msgAt("b1", 1, "b"))

	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(got["plus"]) != 3 || len(got["hash"]) != 3 {
		t.Fatalf("fanout wrong: %+v", got)
	}
	if len(got["b"]) != 1 {
		t.Fatalf("b subscriber got %+v", got["b"])
	}
	for _, seq := range got["plus"] {
		t.Logf("subscriber=plus topic=a/x delivered seq=%d basis=filter a/+ matched", seq)
	}
}

func id(seq uint64) string { return "m-" + uitoa(seq) }

func uitoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// Per-key ordering with a fast subscriber: delivered seq must be ascending.
func TestPerKeyOrdering(t *testing.T) {
	r := testRouter(t, Config{})
	var mu sync.Mutex
	var seqs []uint64
	s, err := r.Subscribe("t", func(_ context.Context, m *Message) error {
		mu.Lock()
		seqs = append(seqs, m.Seq)
		mu.Unlock()
		return nil
	}, WithBufferSize(128))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for seq := uint64(1); seq <= 100; seq++ {
		mustPublish(t, r, msgAt(id(seq), seq, "t"))
	}
	waitUntil(t, time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seqs) == 100
	})
	for i, v := range seqs {
		if v != uint64(i+1) {
			t.Fatalf("order broken at %d: got seq=%d, full=%v", i, v, seqs)
		}
	}
}

func TestDuplicateAndSeqConclusions(t *testing.T) {
	r := testRouter(t, Config{})
	mustPublish(t, r, msgAt("m1", 1, "orders"))
	mustPublish(t, r, msgAt("m2", 2, "orders"))

	// Exact resend: same id, same seq -> duplicate, not redelivered.
	assertReason(t, r, msgAt("m2", 2, "orders"), RejectDuplicate)

	// Same seq with a DIFFERENT id is also a duplicate of the position.
	// Our rule keys duplicate detection on message id; unseen id at an old
	// seq is therefore a rollback — verify both conclusions are distinct.
	assertReason(t, r, msgAt("intruder", 1, "orders"), RejectRollback)

	// Discontinuity: seq jumps ahead -> gap, rejected not silently skipped.
	assertReason(t, r, msgAt("m5", 5, "orders"), RejectGap)

	// Gap does not advance the frontier: seq=3 still accepted afterwards.
	mustPublish(t, r, msgAt("m3", 3, "orders"))

	st := r.shards[0]
	_ = st
	stSnap := r.Snapshot()
	if stSnap.Duplicates != 1 || stSnap.SeqRollbacks != 1 || stSnap.SeqGaps != 1 {
		t.Fatalf("counter breakdown not distinguishable: %+v", stSnap)
	}
	t.Logf("counters after duplicate/rollback/gap: accepted=%d duplicate=%d rollback=%d gap=%d",
		stSnap.Accepted, stSnap.Duplicates, stSnap.SeqRollbacks, stSnap.SeqGaps)
}

func TestDifferentKeysIndependent(t *testing.T) {
	r := testRouter(t, Config{})
	a1 := &Message{Version: Version, ID: "a1", Topic: "evt", Key: "A", Seq: 1}
	b1 := &Message{Version: Version, ID: "b1", Topic: "evt", Key: "B", Seq: 1}
	a2 := &Message{Version: Version, ID: "a2", Topic: "evt", Key: "A", Seq: 2}
	mustPublish(t, r, a1)
	mustPublish(t, r, b1)
	mustPublish(t, r, a2)
	// duplicate within key A only
	assertReason(t, r, &Message{Version: Version, ID: "b1", Topic: "evt", Key: "B", Seq: 1}, RejectDuplicate)
}

func TestPublishAfterCloseRejected(t *testing.T) {
	r := testRouter(t, Config{})
	r.Close()
	res := r.Publish(context.Background(), msgAt("x", 1, "t"))
	if res.Accepted || res.Reason != RejectRouterClosed {
		t.Fatalf("got %+v", res)
	}
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", d)
}
