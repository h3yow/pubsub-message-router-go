package pubsub

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// 支持的 JSON 协议版本。
const supportedJSONVersion = 1

// JSONTransport 解码 JSON 数据报文，形如：
//
//	{"v":1,"topic":"orders/new","key":"orders","seq":1,"payload":"aGVsbG8="}
//
// payload 为原始字节的 base64 编码（encoding/json 默认行为）。
// 拒绝场景：
//   - 报文不是合法 JSON / 含多条报文        -> malformed_frame
//   - v 缺失或不等于 1                      -> unsupported_version
//   - topic / key 缺失                      -> missing_field
//   - topic 含空层级                        -> bad_topic
//   - seq 缺失、为 0 或无法解析             -> bad_sequence
type JSONTransport struct{}

func NewJSONTransport() *JSONTransport { return &JSONTransport{} }

func (t *JSONTransport) Name() string { return "json/1" }

// jsonEnvelope 使用指针字段以区分“字段缺失”与“零值”。
type jsonEnvelope struct {
	V       *int    `json:"v"`
	Topic   *string `json:"topic"`
	Key     *string `json:"key"`
	Seq     *uint64 `json:"seq"`
	Payload []byte  `json:"payload"`
}

func (t *JSONTransport) Decode(frame []byte) (*Message, error) {
	dec := json.NewDecoder(bytes.NewReader(frame))
	var env jsonEnvelope
	if err := dec.Decode(&env); err != nil {
		return nil, &RejectError{
			Reason: RejectMalformed,
			Detail: fmt.Sprintf("invalid JSON frame: %v", err),
		}
	}
	// 同一帧内不得夹带多条 JSON 报文。
	if dec.More() {
		return nil, &RejectError{Reason: RejectMalformed, Detail: "multiple JSON values in one frame"}
	}

	if env.V == nil {
		return nil, &RejectError{Reason: RejectUnsupportedVersion, Detail: "missing protocol version field: v"}
	}
	if *env.V != supportedJSONVersion {
		return nil, &RejectError{
			Reason: RejectUnsupportedVersion,
			Detail: fmt.Sprintf("unsupported version: v=%d, supported: %d", *env.V, supportedJSONVersion),
		}
	}
	if env.Topic == nil {
		return nil, &RejectError{Reason: RejectMissingField, Detail: "missing field: topic"}
	}
	if env.Key == nil {
		return nil, &RejectError{Reason: RejectMissingField, Detail: "missing field: key"}
	}
	if env.Seq == nil {
		return nil, &RejectError{Reason: RejectBadSequence, Detail: "missing field: seq"}
	}
	return normalizeMessage(t.Name(), *env.Topic, *env.Key, *env.Seq, env.Payload)
}
