package pubsub

import (
	"fmt"
	"strconv"
	"strings"
)

// LineTransport 解码长度分隔的行文本协议：
//
//	MSG/1 <topic> <key> <seq> <payload-length>\n
//	<payload，恰好 payload-length 字节，可包含任意字节含换行>
//
// 拒绝场景：
//   - 头部结构无法解析/载荷长度不符/夹带额外字节 -> malformed_frame
//   - 协议名不是 MSG 或版本不是 1               -> unsupported_version
//   - 头部字段不足/载荷被截断                    -> missing_field / malformed_frame
//   - seq 无法解析或为 0                         -> bad_sequence
type LineTransport struct{}

func NewLineTransport() *LineTransport { return &LineTransport{} }

func (t *LineTransport) Name() string { return "line/1" }

func (t *LineTransport) Decode(frame []byte) (*Message, error) {
	nl := strings.IndexByte(string(frame), '\n')
	if nl < 0 {
		return nil, &RejectError{Reason: RejectMalformed, Detail: "missing header terminator '\\n'"}
	}
	header := strings.TrimRight(string(frame[:nl]), "\r")
	body := frame[nl+1:]

	fields := strings.Fields(header)
	if len(fields) == 0 {
		return nil, &RejectError{Reason: RejectMalformed, Detail: "empty frame header"}
	}
	proto := fields[0]
	if !strings.HasPrefix(proto, "MSG/") {
		return nil, &RejectError{
			Reason: RejectUnsupportedVersion,
			Detail: "unknown protocol token: " + proto + ", expected MSG/<version>",
		}
	}
	version := strings.TrimPrefix(proto, "MSG/")
	if version != "1" {
		return nil, &RejectError{
			Reason: RejectUnsupportedVersion,
			Detail: fmt.Sprintf("unsupported line protocol version: %s, supported: 1", version),
		}
	}
	if len(fields) < 5 {
		return nil, &RejectError{
			Reason: RejectMissingField,
			Detail: fmt.Sprintf("header requires 5 fields, got %d", len(fields)),
		}
	}
	topic, key, seqText, lenText := fields[1], fields[2], fields[3], fields[4]

	seq, err := strconv.ParseUint(seqText, 10, 64)
	if err != nil {
		return nil, &RejectError{
			Reason: RejectBadSequence,
			Detail: fmt.Sprintf("unparsable seq %q: %v", seqText, err),
		}
	}
	size, err := strconv.Atoi(lenText)
	if err != nil || size < 0 {
		return nil, &RejectError{
			Reason: RejectMalformed,
			Detail: fmt.Sprintf("invalid payload length %q", lenText),
		}
	}
	if len(body) != size {
		return nil, &RejectError{
			Reason: RejectMalformed,
			Detail: fmt.Sprintf("payload length mismatch: header declares %d, frame contains %d", size, len(body)),
		}
	}

	msg, err := normalizeMessage(t.Name(), topic, key, seq, append([]byte(nil), body...))
	if err != nil {
		return nil, err
	}
	return msg, nil
}
