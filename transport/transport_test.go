package transport

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/h3yow/pubsub-message-router-go"
)

func newTestRouter(t *testing.T) (*router.Router, *RouterIngest) {
	t.Helper()
	r, err := router.New(router.Config{Logger: testLog{t}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r, NewRouterIngest(r)
}

type testLog struct{ t *testing.T }

func (l testLog) Printf(format string, args ...any) { l.t.Logf("[router] "+format, args...) }

func frameJSON(t *testing.T, mutate func(*Frame)) []byte {
	t.Helper()
	f, err := Encode("m1", "a/b", "", 1, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if mutate != nil {
		mutate(&f)
	}
	raw, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestNativeTransportAcceptAndReject(t *testing.T) {
	_, ing := newTestRouter(t)
	native := NewNative(ing)
	ctx := context.Background()

	// Good frame accepted via native transport.
	good := frameJSON(t, nil)
	res := native.Send(ctx, good)
	if !res.Accepted {
		t.Fatalf("good frame rejected: %s %s", res.Code, res.Detail)
	}
	t.Logf("native accepted topic=%s seq=%d id=%s payload=%q", res.Topic, res.Seq, res.ID, "hello")

	// Resend same id -> router-level duplicate, distinguishable from wire errors.
	res = native.Send(ctx, good)
	if res.Accepted || res.Code != CodeRouterRejected || res.Router.Reason != router.RejectDuplicate {
		t.Fatalf("dup: %+v router=%+v", res, res.Router)
	}
	t.Logf("native duplicate verdict code=%s routerReason=%s", res.Code, res.Router.Reason)
}

func TestMalformedFramesDistinguishable(t *testing.T) {
	_, ing := newTestRouter(t)
	ctx := context.Background()

	cases := []struct {
		name string
		raw  []byte
		want RejectCode
	}{
		{"empty", nil, CodeEmptyFrame},
		{"bad json", []byte("{not-json"), CodeMalformedJSON},
		{"unsupported version", frameJSON(t, func(f *Frame) { f.WireVersion = "9.9" }), CodeUnsupportedVersion},
		{"missing id", frameJSON(t, func(f *Frame) { f.ID = "" }), CodeMissingField},
		{"missing topic", frameJSON(t, func(f *Frame) { f.Topic = "" }), CodeMissingField},
		{"zero seq", frameJSON(t, func(f *Frame) { f.Seq = 0 }), CodeMissingField},
		{"bad base64", frameJSON(t, func(f *Frame) { f.Payload = "!!!notb64" }), CodeBadField},
		{"bad crc", frameJSON(t, func(f *Frame) { f.CRC32 = 42 }), CodeChecksumMismatch},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := ing.Ingest(ctx, c.raw)
			if res.Accepted || res.Code != c.want {
				t.Fatalf("want code=%s got accepted=%v code=%s detail=%s",
					c.want, res.Accepted, res.Code, res.Detail)
			}

			t.Logf("frame=%-19s rejected code=%s detail=%s", c.name, res.Code, res.Detail)
		})
	}

	// Corruption in the wild: flip a payload byte but keep the old crc.
	f, _ := Encode("m9", "t", "", 9, []byte("payload-bytes"))
	rawB64 := []byte(f.Payload)
	rawB64[2] = rawB64[2] + 1 // corrupt encoded payload; keep checksum stale
	f.Payload = string(rawB64)
	raw, _ := json.Marshal(f)
	res := ing.Ingest(ctx, raw)
	if res.Accepted || res.Code != CodeChecksumMismatch {
		t.Fatalf("corruption not detected: %+v", res)
	}
}

func TestJSONLineStreamMixed(t *testing.T) {
	_, ing := newTestRouter(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	good1, _ := MarshalFrame(mustFrame(t, "j1", "logs/app", 1, []byte("one")))
	good2, _ := MarshalFrame(mustFrame(t, "j2", "logs/app", 2, []byte("two")))
	stream := strings.Join([]string{
		string(good1),
		"   ",                                    // blank, skipped
		"{broken",                                // malformed
		`{"v":"2","id":"x","topic":"t","seq":1}`, // bad version
		string(good2),
		"",
	}, "\n") + "\n"

	jt := NewJSONLine(ing, bytes.NewBufferString(stream))
	reports, err := jt.Serve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var verdicts []FrameReport
	for rep := range reports {
		verdicts = append(verdicts, rep)
	}
	if len(verdicts) != 4 {
		t.Fatalf("want 4 non-empty verdicts, got %d: %+v", len(verdicts), verdicts)
	}
	if !verdicts[0].Result.Accepted || !verdicts[3].Result.Accepted {
		t.Fatalf("good frames must accept: %+v", verdicts)
	}
	if verdicts[1].Result.Code != CodeMalformedJSON ||
		verdicts[2].Result.Code != CodeUnsupportedVersion {
		t.Fatalf("rejection reasons must be distinguishable, got %+v %+v",
			verdicts[1].Result, verdicts[2].Result)
	}
	// Resending good1 through the stream is a duplicate, not silently dropped.
	res := ing.Ingest(ctx, good1)
	if res.Accepted || res.Router.Reason != router.RejectDuplicate {
		t.Fatalf("want duplicate got %+v", res)
	}
	t.Logf("jsonline verdicts on lines: accept(line1) code=%s(line badjson) code=%s(line badversion) accept(line4)",
		verdicts[1].Result.Code, verdicts[2].Result.Code)
}

func mustFrame(t *testing.T, id, topic string, seq uint64, payload []byte) Frame {
	t.Helper()
	f, err := Encode(id, topic, "", seq, payload)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestFrameTooLarge(t *testing.T) {
	_, ing := newTestRouter(t)
	big := make([]byte, MaxFrameSize+1)
	for i := range big {
		big[i] = 'a'
	}
	res := ing.Ingest(context.Background(), big)
	// Over-long raw data is classified as malformed JSON by direct ingest;
	// the stream reader explicitly emits CodeFrameTooLarge.
	if res.Accepted {
		t.Fatal("big frame accepted")
	}
	jt := NewJSONLine(ing, bytes.NewReader(append(big, '\n')))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rep := <-func() <-chan FrameReport {
		c, _ := jt.Serve(ctx)
		return c
	}()
	if rep.Result.Code != CodeFrameTooLarge {
		t.Fatalf("want frame_too_large, got %s", rep.Result.Code)
	}
	t.Logf("oversize frame line=%d rejected code=%s", rep.Line, rep.Result.Code)
}

func TestChecksumRoundTrip(t *testing.T) {
	payload := []byte{0, 1, 2, 3, 255, 0}
	f, err := Encode("k", "bin", "p1", 7, payload)
	if err != nil {
		t.Fatal(err)
	}
	if base64.StdEncoding.EncodeToString(payload) != f.Payload {
		t.Fatal("payload encoding mismatch")
	}
	if crc32IEEE(payload) != f.CRC32 {
		t.Fatal("crc mismatch")
	}
}
