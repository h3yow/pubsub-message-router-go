package router

import (
	"context"
	"testing"
)

func noopHandler(_ context.Context, _ *Message) error { return nil }

func TestSubscribeValidationAndClose(t *testing.T) {
	r := testRouter(t, Config{})

	if _, err := r.Subscribe("", noopHandler); err == nil {
		t.Fatal("empty filter must error")
	}
	if _, err := r.Subscribe("a/#/c", noopHandler); err == nil {
		t.Fatal("misplaced # must error")
	}

	s, err := r.Subscribe("x", noopHandler, WithDurable("d1"))
	if err != nil {
		t.Fatal(err)
	}
	// Duplicate durable id is rejected.
	if _, err := r.Subscribe("y", noopHandler, WithDurable("d1")); err == nil {
		t.Fatal("duplicate subscriber id must error")
	}
	if r.Snapshot().SubscribersLive != 1 {
		t.Fatalf("live=%d want 1", r.Snapshot().SubscribersLive)
	}
	s.Close()
	if r.Snapshot().SubscribersLive != 0 {
		t.Fatalf("live=%d want 0 after close", r.Snapshot().SubscribersLive)
	}
	t.Logf("lifecycle: invalid filters/dup ids rejected, live counter 1 -> 0 on Close")

	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Subscribe("z", noopHandler); err == nil {
		t.Fatal("subscribe after router close must error")
	}
}
