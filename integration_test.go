package pubsub

import (
	"context"
	"errors"
	"testing"
)

// TestMixedTransportsUnifiedSemantics：JSON 与行文本两种本地传输
// 交替喂入同一路由器，归一化后共享主题路由与每 Key 顺序判定；
// 其中夹杂畸形/不支持版本/缺字段报文，必须给出可区分拒绝，不静默丢弃。
func TestMixedTransportsUnifiedSemantics(t *testing.T) {
	r, _ := New(Config{})
	defer r.Close()
	sub, _ := r.Subscribe("orders/+", 100, PolicyDropNewest)

	jsonTR := NewJSONTransport()
	lineTR := NewLineTransport()
	ctx := context.Background()

	// 交替用两种传输构造同一 Key 的连续序号。
	frames := []struct {
		tr    Transport
		frame []byte
		seq   uint64
	}{
		{jsonTR, []byte(`{"v":1,"topic":"orders/new","key":"o","seq":1}`), 1},
		{lineTR, []byte("MSG/1 orders/new o 2 1\nx"), 2},
		{jsonTR, []byte(`{"v":1,"topic":"orders/new","key":"o","seq":3}`), 3},
		{lineTR, []byte("MSG/1 orders/new o 4 0\n"), 4},
	}
	for _, f := range frames {
		m, err := f.tr.Decode(f.frame)
		if err != nil {
			t.Fatalf("transport decode failed: %v", err)
		}
		res, err := r.Publish(ctx, m)
		if err != nil || res.Verdict != VerdictAccepted {
			t.Fatalf("seq=%d verdict=%s err=%v", f.seq, res.Verdict, err)
		}
		t.Logf("来源=%s topic=%s key=o seq=%d => accepted（两种传输归一后同一顺序域）",
			m.Protocol, m.Topic, m.Seq)
	}

	// 畸形报文：必须被区分拒绝。
	badFrames := []struct {
		tr    Transport
		frame []byte
		want  RejectReason
	}{
		{jsonTR, []byte(`{broken`), RejectMalformed},
		{jsonTR, []byte(`{"v":9,"topic":"orders/new","key":"o","seq":5}`), RejectUnsupportedVersion},
		{lineTR, []byte("MSG/9 orders/new o 5 0\n"), RejectUnsupportedVersion},
		{jsonTR, []byte(`{"v":1,"topic":"orders/new","seq":5}`), RejectMissingField},
		{lineTR, []byte("MSG/1 orders/new o notanum 0\n"), RejectBadSequence},
	}
	for _, bf := range badFrames {
		_, err := bf.tr.Decode(bf.frame)
		var re *RejectError
		if !errors.As(err, &re) || re.Reason != bf.want {
			t.Fatalf("frame=%q want reason=%s got err=%v", bf.frame, bf.want, err)
		}
		t.Logf("拒绝 frame=%q reason=%s（不静默丢弃）", bf.frame, re.Reason)
	}

	// 用两种传输分别重发 seq=2：都必须判 duplicate，且订阅者只收到一次 2。
	for _, tr := range []Transport{jsonTR, lineTR} {
		var republish *Message
		var err error
		if tr.Name() == "json/1" {
			republish, err = tr.Decode([]byte(`{"v":1,"topic":"orders/new","key":"o","seq":2}`))
		} else {
			republish, err = tr.Decode([]byte("MSG/1 orders/new o 2 1\nx"))
		}
		if err != nil {
			t.Fatal(err)
		}
		res, _ := r.Publish(ctx, republish)
		if res.Verdict != VerdictDuplicate {
			t.Fatalf("重发 via %s seq=2 want duplicate got %s", tr.Name(), res.Verdict)
		}
		t.Logf("via=%s 重复 seq=2 => %s expected=%d", tr.Name(), res.Verdict, res.Expected)
	}

	// 订阅者应恰好收到 seq=1..4，各一次。
	for seq := uint64(1); seq <= 4; seq++ {
		m := <-sub.Messages()
		if m.Seq != seq {
			t.Fatalf("订阅者顺序错误 want=%d got=%d", seq, m.Seq)
		}
		t.Logf("投递 topic=%s key=o seq=%d protocol=%s（判定依据：连续序号）",
			m.Topic, m.Seq, m.Protocol)
	}
	select {
	case m := <-sub.Messages():
		t.Fatalf("不应有额外投递, got seq=%d", m.Seq)
	default:
	}
	t.Log("混合传输端到端：订阅者精确收到 seq=1..4，畸形输入全部可区分拒绝")
}
