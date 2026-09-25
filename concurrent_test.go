package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestConcurrentPublishersPerKeyOrder：多个发布者并发发布同一组 Key。
// 序号在发布侧竞争分配、提交顺序天然可能乱序；路由器对乱序给出
// sequence_gap 拒绝，发布者等待前序补齐后重试，直到 accepted。
// 最终验证：不乱序、不丢失、不重复。
func TestConcurrentPublishersPerKeyOrder(t *testing.T) {
	r, _ := New(Config{})
	defer r.Close()

	sub, _ := r.Subscribe("evt/#", 10000, PolicyDropNewest)

	const keys = 8
	const perKey = 200
	const publishers = 8

	var (
		wg       sync.WaitGroup
		verdictM sync.Mutex
		accepted = make(map[string]map[uint64]bool)
		dupCnt   int
		gapCnt   int
	)
	for k := 0; k < keys; k++ {
		accepted["k"+itoa(k)] = make(map[uint64]bool)
	}

	// publishOrdered：遇到 sequence_gap 就等待重试，直到该序号被接受。
	// 前序序号的持有者最终都会补齐，因此等待一定可解除。
	publishOrdered := func(key, topic string, seq uint64) PublishResult {
		deadline := time.Now().Add(5 * time.Second)
		for {
			res, err := r.Publish(context.Background(),
				&Message{Protocol: "test", Topic: topic, Key: key, Seq: seq})
			if err != nil {
				t.Fatalf("publish error key=%s seq=%d: %v", key, seq, err)
			}
			switch res.Verdict {
			case VerdictAccepted:
				return res
			case VerdictGap:
				verdictM.Lock()
				gapCnt++
				verdictM.Unlock()
				if time.Now().After(deadline) {
					t.Fatalf("timeout waiting gap fill key=%s seq=%d", key, seq)
				}
				time.Sleep(time.Millisecond)
				continue
			case VerdictDuplicate:
				verdictM.Lock()
				dupCnt++
				verdictM.Unlock()
				return res
			}
		}
	}

	// 每 Key 一把序号分配锁：模拟有序号来源但多发布者并发提交。
	seqLocks := make([]sync.Mutex, keys)
	nextSeq := make([]uint64, keys)
	var resent sync.Map

	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(pub int) {
			defer wg.Done()
			for round := 0; round < perKey; round++ {
				k := round % keys
				key := "k" + itoa(k)
				seqLocks[k].Lock()
				nextSeq[k]++
				seq := nextSeq[k]
				seqLocks[k].Unlock()

				res := publishOrdered(key, "evt/"+key, seq)
				if res.Verdict == VerdictAccepted {
					verdictM.Lock()
					if accepted[key][seq] {
						t.Errorf("accepted twice key=%s seq=%d", key, seq)
					}
					accepted[key][seq] = true
					verdictM.Unlock()
				}

				// 发布者 0 重发部分序号，必须得到 duplicate 且不重复投递。
				if pub == 0 && seq%10 == 0 {
					token := key + "/" + itoa64(seq)
					if _, loaded := resent.LoadOrStore(token, true); !loaded {
						res2, _ := r.Publish(context.Background(),
							&Message{Protocol: "test", Topic: "evt/" + key, Key: key, Seq: seq})
						if res2.Verdict != VerdictDuplicate {
							t.Errorf("resend key=%s seq=%d verdict=%s want=duplicate", key, seq, res2.Verdict)
						}
						t.Logf("重复消费 key=%s seq=%d => verdict=%s expected=%d（不重复投递）",
							key, seq, res2.Verdict, res2.Expected)
					}
				}
			}
		}(p)
	}
	wg.Wait()
	t.Logf("并发发布完成: 期间空洞重试 gap=%d 重复结论 dup=%d", gapCnt, dupCnt)

	for k := 0; k < keys; k++ {
		key := "k" + itoa(k)
		for seq := uint64(1); seq <= perKey; seq++ {
			if !accepted[key][seq] {
				t.Fatalf("丢失: key=%s seq=%d 未被接受", key, seq)
			}
		}
	}

	// 收集订阅者投递：每 Key 严格升序且数量精确。
	got := make(map[string][]uint64)
	deadline := time.Now().Add(3 * time.Second)
drainLoop:
	for {
		select {
		case m := <-sub.Messages():
			got[m.Key] = append(got[m.Key], m.Seq)
			if allReceived(got, keys, perKey) {
				break drainLoop
			}
		case <-time.After(time.Until(deadline)):
			break drainLoop
		}
	}
	total := 0
	for k := 0; k < keys; k++ {
		key := "k" + itoa(k)
		seqs := got[key]
		if len(seqs) != perKey {
			t.Fatalf("key=%s 投递数=%d want=%d（丢失或重复）", key, len(seqs), perKey)
		}
		for i := 1; i < len(seqs); i++ {
			if seqs[i] <= seqs[i-1] {
				t.Fatalf("key=%s 顺序被破坏: [%d]=%d <= [%d]=%d", key, i, seqs[i], i-1, seqs[i-1])
			}
		}
		total += len(seqs)
		t.Logf("key=%s 订阅者收到 %d 条，严格升序 %d..%d", key, len(seqs), seqs[0], seqs[len(seqs)-1])
	}
	t.Logf("总投递=%d，按 Key 有序、无丢失、无重复", total)
}

func allReceived(got map[string][]uint64, keys, perKey int) bool {
	for k := 0; k < keys; k++ {
		if len(got["k"+itoa(k)]) < perKey {
			return false
		}
	}
	return true
}

func itoa(n int) string { return itoa64(uint64(n)) }
func itoa64(n uint64) string {
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
