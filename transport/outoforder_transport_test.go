package transport

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/pubsub-message-router-go"
)

// The out-of-order window is a router-level capability: the same
// conclusions (hold / accept / release in order) must appear no matter
// which local transport a message arrives on.
func TestOutOfOrderConsistentAcrossTransports(t *testing.T) {
	r, err := router.New(router.Config{
		Logger: testLog{t},
		OutOfOrder: &router.OutOfOrderConfig{
			Match:         func(string, string) bool { return true },
			Capacity:      8,
			MaxWait:       5 * time.Second,
			SweepInterval: 10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })

	var mu sync.Mutex
	var seqs []uint64
	sub, err := r.Subscribe("mix", func(_ context.Context, m *router.Message) error {
		mu.Lock()
		seqs = append(seqs, m.Seq)
		mu.Unlock()
		return nil
	}, router.WithBufferSize(16))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	ing := NewRouterIngest(r)
	native := NewNative(ing)
	ctx := context.Background()

	mk := func(id string, seq uint64) Frame {
		f, err := Encode(id, "mix", "", seq, nil)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}

	// seq=1 via native: accepted.
	res, err := native.SendMessage(ctx, mk("n1", 1))
	if err != nil || !res.Accepted {
		t.Fatalf("seq=1 via native: %+v err=%v", res, err)
	}

	// seq=3 via JSON-line stream: held by the window; the wire report shows
	// the router-level gap_pending conclusion.
	f3, _ := mk("j3", 3), error(nil)
	raw3, err := MarshalFrame(f3)
	if err != nil {
		t.Fatal(err)
	}
	jl := NewJSONLine(ing, strings.NewReader(string(raw3)+"\n"))
	reports, err := jl.Serve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rep, ok := <-reports
	if !ok {
		t.Fatal("no report from jsonline stream")
	}
	if rep.Result.Accepted || rep.Result.Code != CodeRouterRejected ||
		rep.Result.Router.Reason != router.RejectGapPending || !rep.Result.Router.Pending {
		t.Fatalf("seq=3 via jsonline: want gap_pending, got %+v router=%+v", rep.Result, rep.Result.Router)
	}
	t.Logf("jsonline seq=3 held: code=%s router_reason=%s", rep.Result.Code, rep.Result.Router.Reason)

	// seq=2 via native fills the gap: accepted; the held seq=3 is released
	// in order regardless of having arrived on a different transport.
	res, err = native.SendMessage(ctx, mk("n2", 2))
	if err != nil || !res.Accepted {
		t.Fatalf("seq=2 via native: %+v err=%v", res, err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(seqs)
		mu.Unlock()
		if n == 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seqs) != 3 || seqs[0] != 1 || seqs[1] != 2 || seqs[2] != 3 {
		t.Fatalf("delivered seqs=%v, want [1 2 3]", seqs)
	}
	t.Logf("mixed transports delivered seqs=%v strictly increasing, no loss, no dup", seqs)
}
