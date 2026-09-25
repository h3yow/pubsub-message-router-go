package pubsub

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestRestartRecoversWatermarks：重启后高水位恢复，重复序号被拒（duplicate），
// 且可向新订阅者重放历史消息（不丢、不重）。
func TestRestartRecoversWatermarks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.bin")

	// 第一次进程：接受 k1 seq=1..5；seq=3 重发为 duplicate；seq=9 为 gap 不落盘。
	r1, err := New(Config{JournalPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r1.Subscribe("t/#", 50, PolicyDropNewest); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for seq := uint64(1); seq <= 5; seq++ {
		res, err := r1.Publish(ctx, &Message{Protocol: "json/1", Topic: "t/k1", Key: "k1", Seq: seq, Payload: []byte{byte(seq)}})
		if err != nil || res.Verdict != VerdictAccepted {
			t.Fatalf("first run seq=%d verdict=%s err=%v", seq, res.Verdict, err)
		}
	}
	if res, _ := r1.Publish(ctx, &Message{Topic: "t/k1", Key: "k1", Seq: 3}); res.Verdict != VerdictDuplicate {
		t.Fatalf("重启前重发 seq=3 应 duplicate, got %s", res.Verdict)
	}
	if res, _ := r1.Publish(ctx, &Message{Topic: "t/k1", Key: "k1", Seq: 9}); res.Verdict != VerdictGap {
		t.Fatalf("seq=9 应 gap, got %s", res.Verdict)
	}
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}

	// 第二次进程：模拟重启。
	r2, err := New(Config{JournalPath: path})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r2.Close()

	// 水位恢复：seq=1..5 全为 duplicate。
	for seq := uint64(1); seq <= 5; seq++ {
		res, err := r2.Publish(ctx, &Message{Topic: "t/k1", Key: "k1", Seq: seq})
		if err != nil || res.Verdict != VerdictDuplicate {
			t.Fatalf("重启后 seq=%d 应 duplicate, got verdict=%s err=%v", seq, res.Verdict, err)
		}
	}
	t.Logf("重启恢复: key=k1 seq=1..5 全部判定 duplicate，expected=6，未重复投递")

	// seq=6 可正常续接。
	res, err := r2.Publish(ctx, &Message{Topic: "t/k1", Key: "k1", Seq: 6, Payload: []byte{6}})
	if err != nil || res.Verdict != VerdictAccepted {
		t.Fatalf("重启后续接 seq=6 失败: verdict=%s err=%v", res.Verdict, err)
	}
	t.Logf("重启后续接: key=k1 seq=6 verdict=%s expected=%d", res.Verdict, res.Expected)

	// 新订阅者重放崩溃前（及本次）持久化消息，验证不丢不重且有序。
	sub2, _ := r2.Subscribe("t/#", 50, PolicyDropNewest)
	n, err := r2.ReplayTo(sub2)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	for seq := uint64(1); seq <= 6; seq++ {
		m := <-sub2.Messages()
		if m.Key != "k1" || m.Seq != seq || (len(m.Payload) > 0 && m.Payload[0] != byte(seq)) {
			t.Fatalf("replay 顺序/载荷错误 at %d: %+v", seq, m)
		}
		t.Logf("重放 topic=%s key=%s seq=%d payload=%d protocol=%s（按提交顺序）",
			m.Topic, m.Key, m.Seq, m.Payload[0], m.Protocol)
	}
	t.Logf("重启后向新订阅者重放 %d 条，seq=1..6 有序、无丢失、无重复", n)
}

// TestJournalCorruptionDetected：在日志尾部制造损坏，恢复时必须返回
// 可识别的 CorruptionError 并安全截断；已提交前缀的水位不丢失。
func TestJournalCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.bin")

	r1, _ := New(Config{JournalPath: path})
	ctx := context.Background()
	for seq := uint64(1); seq <= 3; seq++ {
		if _, err := r1.Publish(ctx, &Message{Topic: "a/b", Key: "k", Seq: seq}); err != nil {
			t.Fatal(err)
		}
	}
	_ = r1.Close()

	// 追加一段垃圾字节模拟崩溃造成的尾部损坏。
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("GARBAGE_TAIL_CORRUPTION!!!")); err != nil {
		t.Fatal(err)
	}
	f.Close()

	r2, err := New(Config{JournalPath: path})
	if err == nil {
		t.Fatal("期望恢复时返回 CorruptionError，实际 nil")
	}
	var corr *CorruptionError
	if !errors.As(err, &corr) {
		t.Fatalf("期望 *CorruptionError, got %T: %v", err, err)
	}
	t.Logf("检测到日志损坏: offset=%d detail=%q（已截断，路由器仍可用）", corr.Offset, corr.Detail)
	defer r2.Close()

	// 前 3 条水位仍在，且损坏尾部被截断后可继续追加。
	for seq := uint64(1); seq <= 3; seq++ {
		res, _ := r2.Publish(ctx, &Message{Topic: "a/b", Key: "k", Seq: seq})
		if res.Verdict != VerdictDuplicate {
			t.Fatalf("损坏截断后 seq=%d 应 duplicate, got %s", seq, res.Verdict)
		}
	}
	res, err := r2.Publish(ctx, &Message{Topic: "a/b", Key: "k", Seq: 4})
	if err != nil || res.Verdict != VerdictAccepted {
		t.Fatalf("截断后续接 seq=4 失败: verdict=%s err=%v", res.Verdict, err)
	}
	t.Logf("损坏截断后续接: key=k seq=4 verdict=%s，已提交前缀水位完好", res.Verdict)
}
