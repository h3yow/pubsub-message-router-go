package transport_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/pubsub-message-router-go"
	"github.com/h3yow/pubsub-message-router-go/transport"
)

// The reorder disposition must be identical regardless of which local
// transport a message arrives through: run the same gap-fill sequence through
// native direct-call and JSON-line ingest and assert identical verdicts.
func TestReorderVerdictSameAcrossTransports(t *testing.T) {
	policy := router.ReorderPolicy{MaxHeld: 8, MaxWait: time.Second, OnUnfillable: router.GapContinue}
	sel := func(topic, key string) (router.ReorderPolicy, bool) { return policy, true }

	collect := func(t *testing.T, useStream bool) []string {
		r, err := router.New(router.Config{Reorder: sel})
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		ing := transport.NewRouterIngest(r)

		var mu sync.Mutex
		var got []uint64
		sub, err := r.Subscribe("t",
			func(_ context.Context, m *router.Message) error {
				mu.Lock()
				got = append(got, m.Seq)
				mu.Unlock()
				return nil
			}, router.WithBufferSize(64))
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()

		// seq 1, then 3 (held), then 2 (fills).
		frame := func(id string, seq uint64) []byte {
			f, _ := transport.Encode(id, "t", "", seq, nil)
			raw, _ := transport.MarshalFrame(f)
			return raw
		}
		verdicts := make([]string, 0, 3)
		send := func(id string, seq uint64) transport.IngestResult {
			if useStream {
				var buf bytes.Buffer
				buf.Write(frame(id, seq))
				buf.WriteByte('\n')
				jt := transport.NewJSONLine(ing, &buf)
				reps, err := jt.Serve(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				rep := <-reps
				return rep.Result
			}
			return transport.NewNative(ing).Send(context.Background(), frame(id, seq))
		}
		for _, v := range []struct {
			id  string
			seq uint64
		}{{"m1", 1}, {"m3", 3}, {"m2", 2}} {
			res := send(v.id, v.seq)
			switch {
			case res.Router.Held:
				verdicts = append(verdicts, "held")
			case res.Accepted:
				verdicts = append(verdicts, "accepted")
			default:
				verdicts = append(verdicts, "rejected:"+string(res.Code))
			}
		}

		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			n := len(got)
			mu.Unlock()
			if n == 3 {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
			t.Fatalf("delivered=%v", got)
		}
		return verdicts
	}

	native := collect(t, false)
	stream := collect(t, true)
	if len(native) != len(stream) {
		t.Fatalf("verdict length native=%v stream=%v", native, stream)
	}
	for i := range native {
		if native[i] != stream[i] {
			t.Fatalf("verdict differs at %d: native=%v stream=%v", i, native, stream)
		}
	}
	t.Logf("native=%v jsonline=%v -> identical dispositions, delivered 1,2,3 in order", native, stream)
}
