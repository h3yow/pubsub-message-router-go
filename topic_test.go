package pubsub

import "testing"

func TestTopicWildcardMatch(t *testing.T) {
	cases := []struct {
		filter string
		topic  string
		want   bool
	}{
		{"a/b/c", "a/b/c", true},
		{"a/b/c", "a/b/d", false},
		{"a/+/c", "a/b/c", true},
		{"a/+/c", "a/x/c", true},
		{"a/+/c", "a/b/x", false},
		{"a/+/c", "a/b", false},
		{"a/#", "a", true},
		{"a/#", "a/b", true},
		{"a/#", "a/b/c/d", true},
		{"a/#", "x/b/c", false},
		{"+/b/+", "a/b/c", true},
		{"#", "anything/here", true},
		{"orders/+/eu", "orders/new/eu", true},
		{"orders/+/eu", "orders/new/us", false},
	}
	for _, tc := range cases {
		f, err := ParseFilter(tc.filter)
		if err != nil {
			t.Fatalf("ParseFilter(%q) error: %v", tc.filter, err)
		}
		got := f.Match(tc.topic)
		t.Logf("filter=%q topic=%q match=%v want=%v", tc.filter, tc.topic, got, tc.want)
		if got != tc.want {
			t.Errorf("filter=%q topic=%q got=%v want=%v", tc.filter, tc.topic, got, tc.want)
		}
	}
}

func TestTopicInvalidFilter(t *testing.T) {
	bad := []string{"", "a/#/c", "a//b"}
	for _, f := range bad {
		_, err := ParseFilter(f)
		if err == nil {
			t.Errorf("ParseFilter(%q) expected error, got nil", f)
		} else {
			t.Logf("filter=%q rejected reason=%v detail=%v", f, err.(*RejectError).Reason, err)
		}
	}
}
