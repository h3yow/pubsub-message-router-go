package pubsub

import "strings"

// TopicFilter 表示一个订阅主题过滤器，支持 MQTT 风格通配符：
//
//   - "+" 匹配恰好一个主题层级；
//   - "#" 匹配零个或多个层级，且只能出现在过滤器最后一段。
//
// 例："a/+/c" 匹配 "a/b/c"；"a/#" 匹配 "a"、"a/b"、"a/b/c"。
type TopicFilter struct {
	raw    string
	levels []string
}

// ParseFilter 解析并校验主题过滤器。
func ParseFilter(filter string) (*TopicFilter, error) {
	if filter == "" {
		return nil, &RejectError{Reason: RejectMissingField, Detail: "empty topic filter"}
	}
	levels := splitTopic(filter)
	for i, lv := range levels {
		if lv == "#" && i != len(levels)-1 {
			return nil, &RejectError{Reason: RejectBadTopic, Detail: "'#' wildcard must be the last level: " + filter}
		}
		if lv == "" {
			return nil, &RejectError{Reason: RejectBadTopic, Detail: "empty level in filter: " + filter}
		}
	}
	return &TopicFilter{raw: filter, levels: levels}, nil
}

// Raw 返回过滤器原文。
func (f *TopicFilter) Raw() string { return f.raw }

// Match 判断具体主题是否匹配该过滤器。
func (f *TopicFilter) Match(topic string) bool {
	tl := splitTopic(topic)
	return matchLevels(f.levels, tl)
}

func matchLevels(filter, topic []string) bool {
	for i, fl := range filter {
		if fl == "#" {
			// "#" 匹配剩余零个或多个层级。
			return true
		}
		if i >= len(topic) {
			return false
		}
		if fl != "+" && fl != topic[i] {
			return false
		}
	}
	return len(filter) == len(topic)
}

func splitTopic(topic string) []string {
	if topic == "" {
		return nil
	}
	return strings.Split(topic, "/")
}
