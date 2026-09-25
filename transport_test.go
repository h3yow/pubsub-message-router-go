package pubsub

import (
	"errors"
	"testing"
)

func assertRejectReason(t *testing.T, tr Transport, frame []byte, want RejectReason) {
	t.Helper()
	_, err := tr.Decode(frame)
	if err == nil {
		t.Fatalf("%s: frame=%q expected reject %q, got accepted", tr.Name(), string(frame), want)
	}
	var re *RejectError
	if !errors.As(err, &re) {
		t.Fatalf("%s: error is not *RejectError: %v", tr.Name(), err)
	}
	t.Logf("%s frame=%q => reject reason=%q detail=%q", tr.Name(), string(frame), re.Reason, re.Detail)
	if re.Reason != want {
		t.Fatalf("%s: got reason=%q want=%q (detail=%q)", tr.Name(), re.Reason, want, re.Detail)
	}
}

func TestJSONTransportRejects(t *testing.T) {
	tr := NewJSONTransport()
	// 报文损坏
	assertRejectReason(t, tr, []byte("{not json"), RejectMalformed)
	assertRejectReason(t, tr, []byte(`{"v":1}{"v":1}`), RejectMalformed)
	// 版本不支持 / 缺失
	assertRejectReason(t, tr, []byte(`{"topic":"a/b","key":"k","seq":1}`), RejectUnsupportedVersion)
	assertRejectReason(t, tr, []byte(`{"v":2,"topic":"a/b","key":"k","seq":1}`), RejectUnsupportedVersion)
	// 字段缺失
	assertRejectReason(t, tr, []byte(`{"v":1,"key":"k","seq":1}`), RejectMissingField)
	assertRejectReason(t, tr, []byte(`{"v":1,"topic":"a/b","seq":1}`), RejectMissingField)
	// 坏主题
	assertRejectReason(t, tr, []byte(`{"v":1,"topic":"a//b","key":"k","seq":1}`), RejectBadTopic)
	// 坏序号
	assertRejectReason(t, tr, []byte(`{"v":1,"topic":"a/b","key":"k"}`), RejectBadSequence)
	assertRejectReason(t, tr, []byte(`{"v":1,"topic":"a/b","key":"k","seq":0}`), RejectBadSequence)
}

func TestJSONTransportAccepts(t *testing.T) {
	tr := NewJSONTransport()
	m, err := tr.Decode([]byte(`{"v":1,"topic":"orders/new/eu","key":"orders","seq":7,"payload":"aGVsbG8="}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.Protocol != "json/1" || m.Topic != "orders/new/eu" || m.Key != "orders" || m.Seq != 7 || string(m.Payload) != "hello" {
		t.Fatalf("normalized message mismatch: %+v payload=%q", m, m.Payload)
	}
	t.Logf("accepted topic=%s key=%s seq=%d payload=%q protocol=%s", m.Topic, m.Key, m.Seq, m.Payload, m.Protocol)
}

func TestLineTransportRejects(t *testing.T) {
	tr := NewLineTransport()
	// 无换行、头部损坏
	assertRejectReason(t, tr, []byte("MSG/1 a b 1 3"), RejectMalformed)
	assertRejectReason(t, tr, []byte("GARBAGE/1 a b 1 3\nxyz"), RejectUnsupportedVersion)
	assertRejectReason(t, tr, []byte("MSG/2 a b 1 3\nxyz"), RejectUnsupportedVersion)
	// 字段不足
	assertRejectReason(t, tr, []byte("MSG/1 a b 1\n"), RejectMissingField)
	// 坏序号
	assertRejectReason(t, tr, []byte("MSG/1 a/b k xx 3\nabc"), RejectBadSequence)
	assertRejectReason(t, tr, []byte("MSG/1 a/b k 0 3\nabc"), RejectBadSequence)
	// 载荷长度不符（截断 / 多余字节）
	assertRejectReason(t, tr, []byte("MSG/1 a/b k 1 3\nab"), RejectMalformed)
	assertRejectReason(t, tr, []byte("MSG/1 a/b k 1 3\nabcd"), RejectMalformed)
	// 坏主题
	assertRejectReason(t, tr, []byte("MSG/1 a//b k 1 3\nabc"), RejectBadTopic)
}

func TestLineTransportAccepts(t *testing.T) {
	tr := NewLineTransport()
	// payload 含换行也必须能解析（长度分隔）
	m, err := tr.Decode([]byte("MSG/1 orders/new orders 3 5\nab\ncd"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.Protocol != "line/1" || m.Topic != "orders/new" || m.Key != "orders" || m.Seq != 3 || string(m.Payload) != "ab\ncd" {
		t.Fatalf("normalized message mismatch: %+v payload=%q", m, m.Payload)
	}
	t.Logf("accepted topic=%s key=%s seq=%d payload=%q protocol=%s", m.Topic, m.Key, m.Seq, m.Payload, m.Protocol)
}
