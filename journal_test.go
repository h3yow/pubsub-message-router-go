package router

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestJournalRestartFrontierAndNoLoss(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.log")

	// First lifetime: publish seq 1..50.
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	r1 := testRouter(t, Config{Journal: j, RetainPerKey: 100})
	for seq := uint64(1); seq <= 50; seq++ {
		mustPublish(t, r1, msgAt("j"+uitoa(seq), seq, "persisted"))
	}
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}

	// Second lifetime: frontier must be recovered.
	j2, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	r2 := testRouter(t, Config{Journal: j2, RetainPerKey: 100})

	// Old id duplicate still detected after restart.
	assertReason(t, r2, msgAt("j10", 10, "persisted"), RejectDuplicate)
	// rollback and gap still detected against recovered frontier.
	assertReason(t, r2, msgAt("oldX", 3, "persisted"), RejectRollback)
	assertReason(t, r2, msgAt("jump", 99, "persisted"), RejectGap)
	// next in-order message accepted.
	mustPublish(t, r2, msgAt("j51", 51, "persisted"))
	t.Logf("after restart: duplicate/rollback/gap still distinguishable; seq=51 accepted")
}

// A durable subscriber acks seq 1..3 then "crashes" (handler refuses 4..5).
// After router restart, re-subscribing with the same durable id must replay
// exactly seq 4..5 in order (at-least-once; dedup by message id at handler).
func TestDurableSubscriberReplayAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "durable.log")

	// Lifetime 1.
	j, _ := OpenJournal(path)
	r1 := testRouter(t, Config{Journal: j, RetainPerKey: 100})

	lifetime1 := make(chan uint64, 10)
	s1, err := r1.Subscribe("work",
		func(_ context.Context, m *Message) error {
			lifetime1 <- m.Seq
			if m.Seq <= 3 {
				return nil // cursor persists 1..3
			}
			return errors.New("simulated crash: leave cursor at 3")
		},
		WithDurable("worker-1"), WithBufferSize(10))
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 5; seq++ {
		mustPublish(t, r1, msgAt("w"+uitoa(seq), seq, "work"))
	}
	for seq := uint64(1); seq <= 5; seq++ {
		select {
		case <-lifetime1:
		case <-time.After(2 * time.Second):
			t.Fatalf("lifetime1: seq=%d not delivered", seq)
		}
	}
	s1.Close()
	// Allow cursor appends for seq 1..3 to settle (fsync, ordered append).
	time.Sleep(50 * time.Millisecond)
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}

	// Lifetime 2: replay must resume after the durable cursor (seq 3).
	j2, _ := OpenJournal(path)
	r2 := testRouter(t, Config{Journal: j2, RetainPerKey: 100})
	lifetime2 := make(chan uint64, 10)
	var mu sync.Mutex
	var got []uint64
	s2, err := r2.Subscribe("work",
		func(_ context.Context, m *Message) error {
			mu.Lock()
			got = append(got, m.Seq)
			mu.Unlock()
			lifetime2 <- m.Seq
			return nil
		}, WithDurable("worker-1"), WithBufferSize(10))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for seq := uint64(4); seq <= 5; seq++ {
		select {
		case s := <-lifetime2:
			if s != seq {
				t.Fatalf("replay order: want %d got %d (all=%v)", seq, s, got)
			}
			t.Logf("replayed topic=work seq=%d basis=retained seq > durable cursor(3)", s)
		case <-time.After(2 * time.Second):
			t.Fatalf("seq=%d not replayed (got=%v)", seq, got)
		}
	}
}

func TestJournalCorruptionReportedNotSkipped(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.log")
	j, _ := OpenJournal(path)
	r := testRouter(t, Config{Journal: j})
	mustPublish(t, r, msgAt("c1", 1, "x"))
	_ = r.Close()

	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	j2, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(Config{Journal: j2, Logger: testLogger{t}})
	if !errors.Is(err, ErrJournalCorrupt) {
		t.Fatalf("want ErrJournalCorrupt, got %v", err)
	}
	t.Logf("torn frame reported as %v (never silently skipped)", ErrJournalCorrupt)
}
