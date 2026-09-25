// Package transport adapts different local transports to the unified router
// message model. Every adapter normalizes its wire representation into
// *router.Message and, on failure, returns a stable, distinguishable
// RejectCode — malformed input is reported, never silently dropped.
package transport

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/h3yow/pubsub-message-router-go"
)

// FrameVersion is the supported wire-frame protocol version for encoded
// local transports (JSON line, ...).
const FrameVersion = "1.0"

// RejectCode is a stable classification of transport-level rejection.
// Codes are intentionally distinct from router.RejectReason values so logs
// can tell where a message was refused.
type RejectCode string

const (
	CodeOK                 RejectCode = ""
	CodeMalformedJSON      RejectCode = "malformed_json"
	CodeUnsupportedVersion RejectCode = "unsupported_protocol_version"
	CodeMissingField       RejectCode = "missing_field"
	CodeBadField           RejectCode = "bad_field"
	CodeChecksumMismatch   RejectCode = "checksum_mismatch"
	CodeFrameTooLarge      RejectCode = "frame_too_large"
	CodeEmptyFrame         RejectCode = "empty_frame"
	CodeRouterRejected     RejectCode = "router_rejected"
)

// IngestResult reports the disposition of one raw frame. When Accepted is
// false Code identifies the cause and Detail explains it.
type IngestResult struct {
	Accepted bool                 `json:"accepted"`
	Code     RejectCode           `json:"code,omitempty"`
	Detail   string               `json:"detail,omitempty"`
	Topic    string               `json:"topic,omitempty"`
	Seq      uint64               `json:"seq,omitempty"`
	ID       string               `json:"id,omitempty"`
	Router   router.PublishResult `json:"-"`
}

func rejected(code RejectCode, detail string) IngestResult {
	return IngestResult{Accepted: false, Code: code, Detail: detail}
}

// Ingester is the normalized ingress surface: one frame in, one verdict out.
type Ingester interface {
	Ingest(ctx context.Context, raw []byte) IngestResult
}

// Frame is the wire representation shared by the encoded local transports
// (JSON line, unix socket, ...). Payload is base64 standard encoding; crc32
// (IEEE) is optional and, when present, is verified against decoded payload.
type Frame struct {
	WireVersion string `json:"v"`
	ID          string `json:"id"`
	Topic       string `json:"topic"`
	Key         string `json:"key,omitempty"`
	Seq         uint64 `json:"seq"`
	Payload     string `json:"payload,omitempty"`
	CRC32       uint32 `json:"crc32,omitempty"`
}

// RouterIngest adapts a *router.Router to the Ingester interface.
type RouterIngest struct {
	R *router.Router
}

// NewRouterIngest builds an Ingester backed by r.
func NewRouterIngest(r *router.Router) *RouterIngest { return &RouterIngest{R: r} }

// Ingest decodes one normalized Frame, verifies version/checksum/required
// fields, converts it to the unified model and publishes it.
func (a *RouterIngest) Ingest(ctx context.Context, raw []byte) IngestResult {
	if len(raw) == 0 {
		return rejected(CodeEmptyFrame, "frame is empty")
	}
	var f Frame
	if err := json.Unmarshal(raw, &f); err != nil {
		return rejected(CodeMalformedJSON, fmt.Sprintf("parse error: %v", err))
	}
	if f.WireVersion != FrameVersion {
		return rejected(CodeUnsupportedVersion,
			fmt.Sprintf("frame v=%q supported=%q", f.WireVersion, FrameVersion))
	}
	if f.ID == "" {
		return rejected(CodeMissingField, "field 'id' is required")
	}
	if f.Topic == "" {
		return rejected(CodeMissingField, "field 'topic' is required")
	}
	if f.Seq == 0 {
		return rejected(CodeMissingField, "field 'seq' must be >= 1")
	}
	var payload []byte
	if f.Payload != "" {
		dec, err := base64.StdEncoding.DecodeString(f.Payload)
		if err != nil {
			return rejected(CodeBadField, fmt.Sprintf("payload is not valid base64: %v", err))
		}
		if f.CRC32 != 0 && crc32IEEE(dec) != f.CRC32 {
			return rejected(CodeChecksumMismatch,
				fmt.Sprintf("topic=%s seq=%d id=%s: crc32 mismatch, frame corrupt",
					f.Topic, f.Seq, f.ID))
		}
		payload = dec
	}
	msg := &router.Message{
		Version: router.Version,
		ID:      f.ID,
		Topic:   f.Topic,
		Key:     f.Key,
		Seq:     f.Seq,
		Payload: payload,
	}
	res := a.R.Publish(ctx, msg)
	out := IngestResult{
		Accepted: res.Accepted,
		Topic:    f.Topic,
		Seq:      f.Seq,
		ID:       f.ID,
		Router:   res,
	}
	if !res.Accepted {
		out.Code = CodeRouterRejected
		out.Detail = fmt.Sprintf("reason=%s detail=%s", res.Reason, res.Detail)
	}
	return out
}

var _ Ingester = (*RouterIngest)(nil)
