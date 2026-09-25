package router_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/h3yow/pubsub-message-router-go"
	"github.com/h3yow/pubsub-message-router-go/transport"
)

// Demonstrates two local transports (in-process native + JSON-line stream)
// normalized into one router, with wildcard routing and backpressure stats.
func Example_twoTransports_normalized() {
	r, _ := router.New(router.Config{Logger: logfn(func(s string) { fmt.Println(s) })})
	defer r.Close()
	ing := transport.NewRouterIngest(r)

	var mu sync.Mutex
	count := 0
	sub, _ := r.Subscribe("sensor/+/temp",
		func(_ context.Context, m *router.Message) error {
			mu.Lock()
			count++
			mu.Unlock()
			return nil
		}, router.WithBufferSize(64))
	defer sub.Close()

	ctx := context.Background()
	native := transport.NewNative(ing)

	// Transport A: direct in-process calls.
	for _, dev := range []string{"a", "b"} {
		f, _ := transport.Encode(fmt.Sprintf("native-%s", dev),
			"sensor/"+dev+"/temp", "", 1, []byte("21.5"))
		raw, _ := transport.MarshalFrame(f)
		res := native.Send(ctx, raw)
		fmt.Println("native:", verdict(res.Accepted, res.Code))
	}

	// Transport B: newline JSON stream (e.g. pipe from a local agent).
	stream := &bytes.Buffer{}
	for seq := uint64(1); seq <= 2; seq++ {
		f, _ := transport.Encode(fmt.Sprintf("stream-%d", seq),
			"sensor/c/temp", "", seq, []byte("19.0"))
		raw, _ := transport.MarshalFrame(f)
		stream.Write(raw)
		stream.WriteByte('\n')
	}
	jt := transport.NewJSONLine(ing, stream)
	reports, _ := jt.Serve(ctx)
	for rep := range reports {
		fmt.Printf("stream line=%d: %s\n", rep.Line, verdict(rep.Result.Accepted, rep.Result.Code))
	}

	time.Sleep(30 * time.Millisecond)
	mu.Lock()
	fmt.Println("total delivered:", count)
	mu.Unlock()
	fmt.Println("snapshot accepted:", r.Snapshot().Accepted)

	// Output:
	// native: accepted
	// native: accepted
	// stream line=1: accepted
	// stream line=2: accepted
	// total delivered: 4
	// snapshot accepted: 4
}

func verdict(accepted bool, code transport.RejectCode) string {
	if accepted {
		return "accepted"
	}
	return "rejected:" + string(code)
}

type logfn func(string)

func (f logfn) Printf(format string, args ...any) {
	// Keep example output deterministic: silence the internal logs.
}
