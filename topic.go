package router

import "strings"

// topicKey identifies the ordering/dedup partition.
// Two messages with the same (topic, key) are strictly ordered relative to
// each other; messages on different partitions are independent.
type topicKey struct {
	topic string
	key   string
}

// MatchTopic reports whether a concrete topic matches a subscription filter.
//
// Matching is MQTT-style over '/'-separated segments:
//   - "+" matches exactly one segment (e.g. "a/+" matches "a/b", not "a/b/c")
//   - "#" matches zero or more trailing segments and may only appear as the
//     last segment (e.g. "a/#" matches "a", "a/b", "a/b/c")
//   - everything else compares literally (case-sensitive)
//
// An invalid filter placement of '#' (not the final segment) never matches.
func MatchTopic(filter, topic string) bool {
	if filter == "" || topic == "" {
		return false
	}
	if filter == "#" {
		return true
	}
	fs := strings.Split(filter, "/")
	ts := strings.Split(topic, "/")
	for i, fseg := range fs {
		if fseg == "#" {
			// '#' is only legal as the final segment; it matches the rest
			// of the topic, including a trailing boundary ("a/#" ~ "a").
			return i == len(fs)-1
		}
		if i >= len(ts) {
			return false
		}
		if fseg != "+" && fseg != ts[i] {
			return false
		}
	}
	return len(fs) == len(ts)
}
