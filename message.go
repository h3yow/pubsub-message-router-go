package router

import (
	"errors"
	"fmt"
)

// Version is the only supported normalized message protocol version.
// Transport adapters are responsible for mapping their own wire versions
// onto the normalized model; frames advertising an unsupported transport
// version are rejected before a Message is constructed.
const Version = "1.0"

// Message is the unified, transport-independent message model.
//
// Every local transport (native direct call, JSON-line stream, ...) is
// normalized into this struct before it reaches routing logic.
type Message struct {
	// Version of the normalized model. Must equal Version.
	Version string `json:"version"`
	// ID is the producer-assigned globally unique message id.
	// It is the basis for idempotent duplicate suppression.
	ID string `json:"id"`
	// Topic is the concrete (non-wildcard) topic the message is published to.
	Topic string `json:"topic"`
	// Key optionally narrows the ordering/dedup partition. When empty the
	// topic itself is the partition key: order is preserved per (topic,key).
	Key string `json:"key,omitempty"`
	// Seq is the producer-assigned, gap-free, strictly increasing per-key
	// sequence number starting at 1. Used to detect duplicates, rollbacks
	// and gaps explicitly instead of silently skipping.
	Seq uint64 `json:"seq"`
	// Payload is the opaque application body. May be empty.
	Payload []byte `json:"payload,omitempty"`
}

// partitionKey returns the ordering/dedup partition key.
func (m *Message) partitionKey() topicKey {
	k := m.Key
	if k == "" {
		k = m.Topic
	}
	return topicKey{topic: m.Topic, key: k}
}

// RejectReason is a stable, distinguishable classification of why a publish
// was not accepted. It is logged and surfaced in PublishResult so that
// callers never have to infer disposition from a missing delivery.
type RejectReason string

const (
	RejectNone RejectReason = ""

	// Normalized-model validation failures.
	RejectUnsupportedVersion RejectReason = "unsupported_version"
	RejectMissingID          RejectReason = "missing_id"
	RejectMissingTopic       RejectReason = "missing_topic"
	RejectZeroSeq            RejectReason = "zero_seq"

	// Sequence disposition (explicit, distinguishable conclusions).
	RejectDuplicate RejectReason = "duplicate"
	RejectRollback  RejectReason = "seq_rollback"
	RejectGap       RejectReason = "seq_gap"

	// Lifecycle.
	RejectRouterClosed RejectReason = "router_closed"
)

// ErrRejected wraps a rejection with its reason and explanatory detail.
type ErrRejected struct {
	Reason RejectReason
	Detail string
}

func (e *ErrRejected) Error() string {
	return fmt.Sprintf("message rejected: %s: %s", e.Reason, e.Detail)
}

// AsReject extracts *ErrRejected from err if present.
func AsReject(err error) (*ErrRejected, bool) {
	var r *ErrRejected
	if errors.As(err, &r) {
		return r, true
	}
	return nil, false
}

// Validate checks the normalized fields independent of any transport.
// Every failure maps to a distinct RejectReason so malformed input is
// never silently dropped.
func (m *Message) Validate() error {
	if m == nil {
		return &ErrRejected{Reason: RejectMissingID, Detail: "nil message"}
	}
	if m.Version != Version {
		return &ErrRejected{
			Reason: RejectUnsupportedVersion,
			Detail: fmt.Sprintf("version=%q supported=%q", m.Version, Version),
		}
	}
	if m.ID == "" {
		return &ErrRejected{Reason: RejectMissingID, Detail: "message id is required"}
	}
	if m.Topic == "" {
		return &ErrRejected{Reason: RejectMissingTopic, Detail: "topic is required"}
	}
	if m.Seq == 0 {
		return &ErrRejected{Reason: RejectZeroSeq, Detail: fmt.Sprintf("topic=%s id=%s seq must be >= 1", m.Topic, m.ID)}
	}
	return nil
}

// PublishResult explains the disposition of every publish attempt.
//
// Accepted=true  -> the message is new and durably committed; Delivered is
//
//	the number of live subscribers it was offered to and
//	Dropped counts back-pressure drops among them.
//
// Accepted=false -> Reason gives the precise conclusion; a duplicate is not
//
//	an error condition requiring retry, a rollback/gap is.
type PublishResult struct {
	Accepted  bool
	Reason    RejectReason
	Detail    string
	Message   *Message
	Delivered int
	Dropped   int
}

func (r PublishResult) String() string {
	if r.Accepted {
		m := r.Message
		return fmt.Sprintf("ACCEPTED topic=%s key=%s seq=%d id=%s delivered=%d dropped=%d",
			m.Topic, m.Key, m.Seq, m.ID, r.Delivered, r.Dropped)
	}
	return fmt.Sprintf("REJECTED reason=%s detail=%s", r.Reason, r.Detail)
}
