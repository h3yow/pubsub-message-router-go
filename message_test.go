package router

import "testing"

func TestMessageValidate(t *testing.T) {
	cases := []struct {
		name string
		m    *Message
		want RejectReason
	}{
		{"valid", msgAt("a", 1, "t/x"), RejectNone},
		{"bad version", &Message{Version: "0.9", ID: "a", Topic: "t", Seq: 1}, RejectUnsupportedVersion},
		{"missing id", &Message{Version: Version, Topic: "t", Seq: 1}, RejectMissingID},
		{"missing topic", &Message{Version: Version, ID: "a", Seq: 1}, RejectMissingTopic},
		{"zero seq", &Message{Version: Version, ID: "a", Topic: "t"}, RejectZeroSeq},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.m.Validate()
			if c.want == RejectNone {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			er, ok := AsReject(err)
			if !ok {
				t.Fatalf("want ErrRejected, got %v", err)
			}
			if er.Reason != c.want {
				t.Fatalf("want reason=%s got=%s detail=%s", c.want, er.Reason, er.Detail)
			}
			t.Logf("topic=%q seq=%d rejected as %s: %s", c.m.Topic, c.m.Seq, er.Reason, er.Detail)
		})
	}
}

func TestMatchTopic(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"a/b/c", "a/b/c", true},
		{"a/b/c", "a/b/d", false},
		{"a/+", "a/b", true},
		{"a/+", "a/b/c", false},
		{"+/b/+", "a/b/c", true},
		{"a/#", "a", true},
		{"a/#", "a/b/c", true},
		{"#", "anything/at/all", true},
		{"a/#/c", "a/b/c", false}, // misplaced #
		{"a/b", "a", false},
		{"", "a", false},
	}
	for _, c := range cases {
		got := MatchTopic(c.filter, c.topic)
		if got != c.want {
			t.Errorf("MatchTopic(%q,%q)=%v want %v", c.filter, c.topic, got, c.want)
		}
	}
}
