package pubsub

import (
	"context"
	"testing"
)

func msg(topic, key string, seq uint64) *Message {
	return &Message{Protocol: "test", Topic: topic, Key: key, Seq: seq, Payload: []byte("p")}
}

func TestPublishSequenceVerdicts(t *testing.T) {
	r, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	sub, err := r.Subscribe("a/b", 10, PolicyDropNewest)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	publish := func(seq uint64, want PublishVerdict) {
		t.Helper()
		res, err := r.Publish(ctx, msg("a/b", "k1", seq))
		if err != nil {
			t.Fatalf("seq=%d unexpected error: %v", seq, err)
		}
		t.Logf("topic=a/b key=k1 seq=%d => %s (判定依据: expected=%d lastAccepted=%d)",
			seq, res, res.Expected, res.LastAccepted)
		if res.Verdict != want {
			t.Fatalf("seq=%d verdict=%s want=%s", seq, res.Verdict, want)
		}
	}

	publish(1, VerdictAccepted) // 首条 expected=1
	publish(2, VerdictAccepted)
	publish(2, VerdictDuplicate) // 重复消费
	publish(1, VerdictDuplicate) // 序号回退
	publish(5, VerdictGap)       // 空洞 3,4 缺失
	publish(3, VerdictAccepted)  // 补齐空洞后可继续
	publish(4, VerdictAccepted)
	publish(5, VerdictAccepted) // 空洞补齐后，5 现在可接受
	publish(6, VerdictAccepted)
	publish(6, VerdictDuplicate) // 再次消费 6 判定重复
	_ = sub
}

func TestPerKeyOrderingAndRouting(t *testing.T) {
	r, _ := New(Config{})
	defer r.Close()
	subAB, _ := r.Subscribe("a/+", 100, PolicyDropNewest)
	subAll, _ := r.Subscribe("#", 100, PolicyDropNewest)
	subC, _ := r.Subscribe("c", 100, PolicyDropNewest)

	ctx := context.Background()
	for seq := uint64(1); seq <= 50; seq++ {
		if _, err := r.Publish(ctx, msg("a/b", "k1", seq)); err != nil {
			t.Fatal(err)
		}
	}

	// subAB 与 subAll 应各收到 1..50 严格有序；subC 不应收到。
	for seq := uint64(1); seq <= 50; seq++ {
		got := <-subAB.Messages()
		if got.Key != "k1" || got.Seq != seq {
			t.Fatalf("subAB order broken at %d: got key=%s seq=%d", seq, got.Key, got.Seq)
		}
		gotAll := <-subAll.Messages()
		if gotAll.Seq != seq {
			t.Fatalf("subAll order broken at %d: got seq=%d", seq, gotAll.Seq)
		}
	}
	select {
	case m := <-subC.Messages():
		t.Fatalf("subC unexpectedly received topic=%s seq=%d", m.Topic, m.Seq)
	default:
	}
	t.Logf("两个匹配订阅者均按 key=k1 收到 seq=1..50，非匹配订阅者零投递，顺序一致")
}
