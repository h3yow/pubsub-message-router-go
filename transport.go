package pubsub

// RejectReason 是可区分的输入拒绝原因。
type RejectReason string

const (
	// RejectMalformed 报文结构损坏、无法解析。
	RejectMalformed RejectReason = "malformed_frame"
	// RejectUnsupportedVersion 协议版本不支持。
	RejectUnsupportedVersion RejectReason = "unsupported_version"
	// RejectMissingField 必填字段缺失。
	RejectMissingField RejectReason = "missing_field"
	// RejectBadTopic 主题非法。
	RejectBadTopic RejectReason = "bad_topic"
	// RejectBadSequence 序号非法（0 或无法解析）。
	RejectBadSequence RejectReason = "bad_sequence"
)

// RejectError 携带可区分的拒绝原因与明细，绝不静默丢弃。
type RejectError struct {
	Reason RejectReason
	Detail string
}

func (e *RejectError) Error() string {
	return "transport reject: " + string(e.Reason) + ": " + e.Detail
}

// Transport 是本地传输适配器：把原始报文解码为统一 Message。
type Transport interface {
	Name() string
	Decode(frame []byte) (*Message, error)
}

// normalizeMessage 对解码后的字段做统一校验，任何非法输入都返回
// 携带 RejectReason 的错误，保证调用方能区分拒绝原因。
func normalizeMessage(proto, topic, key string, seq uint64, payload []byte) (*Message, error) {
	if topic == "" {
		return nil, &RejectError{Reason: RejectMissingField, Detail: "missing field: topic"}
	}
	for _, lv := range splitTopic(topic) {
		if lv == "" {
			return nil, &RejectError{Reason: RejectBadTopic, Detail: "empty level in topic: " + topic}
		}
	}
	if key == "" {
		return nil, &RejectError{Reason: RejectMissingField, Detail: "missing field: key"}
	}
	if seq == 0 {
		return nil, &RejectError{Reason: RejectBadSequence, Detail: "seq must be >= 1, got 0"}
	}
	return &Message{
		Protocol: proto,
		Topic:    topic,
		Key:      key,
		Seq:      seq,
		Payload:  payload,
	}, nil
}
