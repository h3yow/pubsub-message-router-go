package router

import (
	"fmt"
	"testing"
)

// testLogger prints every disposition line with topic/seq/basis so a failing
// test is explainable from its logs.
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Logf("[router] "+format, args...)
}

func testRouter(t *testing.T, cfg Config) *Router {
	t.Helper()
	if cfg.Logger == nil {
		cfg.Logger = testLogger{t: t}
	}
	r, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func msgAt(id string, seq uint64, topic string) *Message {
	return &Message{Version: Version, ID: id, Topic: topic, Seq: seq}
}

// mustPublish asserts acceptance and logs the basis.
func mustPublish(t *testing.T, r *Router, m *Message) PublishResult {
	t.Helper()
	res := r.Publish(t.Context(), m)
	if !res.Accepted {
		t.Fatalf("expected accept topic=%s seq=%d id=%s, got reason=%s detail=%s",
			m.Topic, m.Seq, m.ID, res.Reason, res.Detail)
	}
	return res
}

func assertReason(t *testing.T, r *Router, m *Message, want RejectReason) PublishResult {
	t.Helper()
	res := r.Publish(t.Context(), m)
	if res.Accepted || res.Reason != want {
		t.Fatalf("topic=%s seq=%d id=%s: want reject=%s got accepted=%v reason=%s detail=%s",
			m.Topic, m.Seq, m.ID, want, res.Accepted, res.Reason, res.Detail)
	}
	return res
}

var _ = fmt.Sprintf
