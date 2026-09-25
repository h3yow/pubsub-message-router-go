package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestSlowSubscriberDoesNotBlockOthers：慢订阅者缓冲打满后，
// 发布者与其他正常订阅者都不应被阻塞。
func TestSlowSubscriberDoesNotBlockOthers(t *testing.T) {
	r, _ := New(Config{})
	defer r.Close()

	slow, _ := r.Subscribe("t", 2, PolicyDropNewest) // 慢，缓冲 2，从不读取
	fast, _ := r.Subscribe("t", 100, PolicyDropNewest)

	var (
		events []OverflowEvent
		eventM sync.Mutex
	)
	r.OnOverflow(func(ev OverflowEvent) {
		eventM.Lock()
		events = append(events, ev)
		eventM.Unlock()
	})

	// 远大于慢订阅者容量；Publish 必须始终非阻塞。
	done := make(chan struct{})
	go func() {
		defer close(done)
		for seq := uint64(1); seq <= 50; seq++ {
			res, err := r.Publish(context.Background(),
				&Message{Protocol: "test", Topic: "t", Key: "k", Seq: seq})
			if err != nil || res.Verdict != VerdictAccepted {
				t.Errorf("seq=%d blocked/rejected: verdict=%s err=%v", seq, res.Verdict, err)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("慢订阅者阻塞了发布者：Publish 未能在限定时间内完成")
	}

	// 快订阅者收到全部 50 条。
	for seq := uint64(1); seq <= 50; seq++ {
		select {
		case m := <-fast.Messages():
			if m.Seq != seq {
				t.Fatalf("fast 订阅者乱序: want=%d got=%d", seq, m.Seq)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("fast 订阅者在 seq=%d 处被慢订阅者拖累", seq)
		}
	}

	// 慢订阅者缓冲恰好 2 条（最早 2 条，新消息被丢）。
	first := <-slow.Messages()
	second := <-slow.Messages()
	if first.Seq != 1 || second.Seq != 2 {
		t.Fatalf("DropNewest 应保留最旧消息，got %d,%d", first.Seq, second.Seq)
	}
	select {
	case m := <-slow.Messages():
		t.Fatalf("慢订阅者不应有第三条消息, got seq=%d", m.Seq)
	default:
	}

	st, _ := r.Stats(slow.ID())
	eventM.Lock()
	n := len(events)
	sample := OverflowEvent{}
	if n > 0 {
		sample = events[0]
	}
	eventM.Unlock()
	if st.Delivered != 2 || st.DroppedNewest != 48 {
		t.Fatalf("慢订阅者计数异常: %+v", st)
	}
	if n == 0 {
		t.Fatal("期望观测到溢出事件，实际没有")
	}
	t.Logf("慢订阅者 stats: delivered=%d droppedNewest=%d；溢出事件 %d 个，样例 sub=%s topic=%s seq=%d policy=%d",
		st.Delivered, st.DroppedNewest, n, sample.SubscriptionID, sample.Topic, sample.Seq, sample.Policy)
	t.Logf("快订阅者收到全部 50 条且有序，发布者全程未被阻塞")
}

// TestOverflowPolicyDropOldest：挤出最旧，缓冲最终保留最新消息。
func TestOverflowPolicyDropOldest(t *testing.T) {
	r, _ := New(Config{})
	defer r.Close()
	sub, _ := r.Subscribe("t", 3, PolicyDropOldest)

	for seq := uint64(1); seq <= 10; seq++ {
		if _, err := r.Publish(context.Background(),
			&Message{Protocol: "test", Topic: "t", Key: "k", Seq: seq}); err != nil {
			t.Fatal(err)
		}
	}
	var got []uint64
	for {
		select {
		case m := <-sub.Messages():
			got = append(got, m.Seq)
		default:
			goto done
		}
	}
done:
	// 容量 3，最终应保留最新三条 8,9,10，且顺序正确。
	if len(got) != 3 || got[0] != 8 || got[1] != 9 || got[2] != 10 {
		t.Fatalf("DropOldest 期望保留 [8 9 10], got %v", got)
	}
	st, _ := r.Stats(sub.ID())
	t.Logf("DropOldest: 缓冲内 seq=%v, droppedOldest=%d delivered=%d（溢出显式可观测）", got, st.DroppedOldest, st.Delivered)
}

// TestOverflowPolicyFailSubscriber：溢出即失败，通道关闭并被路由器摘除。
func TestOverflowPolicyFailSubscriber(t *testing.T) {
	r, _ := New(Config{})
	defer r.Close()
	sub, _ := r.Subscribe("t", 2, PolicyFailSubscriber)

	var failedEv OverflowEvent
	var evOnce sync.Once
	r.OnOverflow(func(ev OverflowEvent) {
		evOnce.Do(func() { failedEv = ev })
	})

	var lastVerdict PublishResult
	for seq := uint64(1); seq <= 5; seq++ {
		res, err := r.Publish(context.Background(),
			&Message{Protocol: "test", Topic: "t", Key: "k", Seq: seq})
		if err != nil {
			t.Fatal(err)
		}
		lastVerdict = res
	}

	// 排空关闭前已入队的缓冲消息后，通道应处于关闭状态。
	drained := 0
drainClosed:
	for {
		select {
		case _, ok := <-sub.Messages():
			if !ok {
				break drainClosed
			}
			drained++
		default:
			t.Fatal("订阅者应在排空少量缓冲后关闭，但通道仍开放")
		}
	}
	t.Logf("排空失败前已入队缓冲 %d 条后通道关闭", drained)
	select {
	case <-sub.Done():
	default:
		t.Fatal("Done 通道应已关闭")
	}
	// 路由器摘除后，Stats 找不到该订阅者；后续发布不报错（不阻塞发布者）。
	if _, ok := r.Stats(sub.ID()); ok {
		t.Fatal("失败订阅者应已从路由器摘除")
	}
	res, err := r.Publish(context.Background(),
		&Message{Protocol: "test", Topic: "t", Key: "k", Seq: lastVerdict.Seq + 1})
	if err != nil || res.Verdict != VerdictAccepted {
		t.Fatalf("摘除失败订阅者后发布应正常: verdict=%s err=%v", res.Verdict, err)
	}
	st, _ := r.Stats(sub.ID())
	t.Logf("FailSubscriber: 溢出事件 sub=%s seq=%d policy=%d；订阅者关闭摘除，后续 seq=%d 正常 accepted",
		failedEv.SubscriptionID, failedEv.Seq, failedEv.Policy, res.Seq)
	_ = st
}
