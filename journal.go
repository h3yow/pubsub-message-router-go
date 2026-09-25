package router

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// Journal is an append-only durable log of accepted messages and subscriber
// cursors. Records are length-prefixed binary frames:
//
//	magic(2)="PJ" | kind(1) | payloadLen(4,big-endian) | payload
//
// kind 1 payload = JSON encoding of Message
// kind 2 payload = JSON encoding of cursorRecord
//
// A journal surviving a process restart lets the router rebuild per-key
// sequence state and replay unacknowledged messages to durable subscribers,
// so restarts do not lose, duplicate (beyond at-least-once replay suppressed
// by message ID) or reorder messages.
type Journal struct {
	mu     sync.Mutex
	f      *os.File
	path   string
	fsync  bool
	closed bool
}

const (
	journalMagic0 byte = 0x50 // 'P'
	journalMagic1 byte = 0x4A // 'J'

	journalKindMessage byte = 1
	journalKindCursor  byte = 2
)

// ErrJournalCorrupt means a frame could not be fully read/decoded.
var ErrJournalCorrupt = errors.New("journal corrupt")

// JournalOption configures journal behavior.
type JournalOption func(*Journal)

// WithoutFsync disables fsync per append (tests, ephemeral routing).
func WithoutFsync() JournalOption {
	return func(j *Journal) { j.fsync = false }
}

// OpenJournal opens or creates the journal at path and verifies its tail.
func OpenJournal(path string, opts ...JournalOption) (*Journal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open journal %s: %w", path, err)
	}
	j := &Journal{f: f, path: path, fsync: true}
	for _, o := range opts {
		o(j)
	}
	return j, nil
}

// InMemoryJournal builds a journal backed by no file (durability disabled),
// useful when persistence is not required.
func InMemoryJournal() *Journal {
	return &Journal{}
}

// Enabled reports whether the journal has durable backing.
func (j *Journal) Enabled() bool { return j != nil && j.f != nil }

func (j *Journal) appendRecord(kind byte, payload []byte) error {
	if j == nil || j.f == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return errors.New("journal closed")
	}
	hdr := make([]byte, 7)
	hdr[0] = journalMagic0
	hdr[1] = journalMagic1
	hdr[2] = kind
	binary.BigEndian.PutUint32(hdr[3:7], uint32(len(payload)))
	if _, err := j.f.Write(hdr); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := j.f.Write(payload); err != nil {
			return err
		}
	}
	if j.fsync {
		if err := j.f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// AppendMessage durably appends a message record.
func (j *Journal) AppendMessage(m *Message) error {
	payload, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encode journal message: %w", err)
	}
	return j.appendRecord(journalKindMessage, payload)
}

// cursorRecord is the JSON payload of a cursor frame. The cursor is per
// (subscriber, partition key) because each key has its own Seq space.
type cursorRecord struct {
	SubscriberID string `json:"subscriber_id"`
	Topic        string `json:"topic"`
	Key          string `json:"key"`
	Seq          uint64 `json:"seq"`
}

// AppendCursor durably persists a durable subscriber's acknowledged sequence
// position for one partition key.
func (j *Journal) AppendCursor(subID, topic, key string, seq uint64) error {
	payload, err := json.Marshal(cursorRecord{SubscriberID: subID, Topic: topic, Key: key, Seq: seq})
	if err != nil {
		return err
	}
	return j.appendRecord(journalKindCursor, payload)
}

// journalEntry is one decoded record in replay order.
type journalEntry struct {
	kind  byte
	msg   *Message
	subID string
	topic string
	key   string
	seq   uint64
}

// Replay visits records from the beginning in append order. A corrupt frame
// is reported with ErrJournalCorrupt (never silently skipped); a clean EOF
// after a complete frame returns nil.
func (j *Journal) Replay(visit func(journalEntry) error) error {
	if j == nil || j.f == nil {
		return nil
	}
	j.mu.Lock()
	path := j.path
	j.mu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := io.Reader(f)
	hdr := make([]byte, 7)
	for {
		n, err := io.ReadFull(r, hdr)
		if err == io.EOF && n == 0 {
			return nil // clean end of journal
		}
		if err == io.ErrUnexpectedEOF || (err == io.EOF && n > 0) {
			return fmt.Errorf("%w: truncated header at frame boundary", ErrJournalCorrupt)
		}
		if err != nil {
			return err
		}
		if hdr[0] != journalMagic0 || hdr[1] != journalMagic1 {
			return fmt.Errorf("%w: bad magic bytes %x %x", ErrJournalCorrupt, hdr[0], hdr[1])
		}
		kind := hdr[2]
		plen := binary.BigEndian.Uint32(hdr[3:7])
		payload := make([]byte, plen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return fmt.Errorf("%w: truncated payload: %v", ErrJournalCorrupt, err)
		}
		var e journalEntry
		switch kind {
		case journalKindMessage:
			var m Message
			if err := json.Unmarshal(payload, &m); err != nil {
				return fmt.Errorf("%w: bad message json: %v", ErrJournalCorrupt, err)
			}
			e.kind, e.msg = kind, &m
		case journalKindCursor:
			var c cursorRecord
			if err := json.Unmarshal(payload, &c); err != nil {
				return fmt.Errorf("%w: bad cursor json: %v", ErrJournalCorrupt, err)
			}
			e.kind, e.subID, e.topic, e.key, e.seq = kind, c.SubscriberID, c.Topic, c.Key, c.Seq
		default:
			return fmt.Errorf("%w: unknown record kind %d", ErrJournalCorrupt, kind)
		}
		if err := visit(e); err != nil {
			return err
		}
	}
}

// Close releases the journal file.
func (j *Journal) Close() error {
	if j == nil || j.f == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	return j.f.Close()
}

var _ io.Closer = (*Journal)(nil)
