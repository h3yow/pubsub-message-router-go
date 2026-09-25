package router

import (
	"context"
	"sync"
)

// OverflowPolicy decides what happens when a subscriber's bounded buffer is
// full. The policy is fixed at subscription time so behavior is explicit;
// every invocation is reported through OverflowCallback and counted.
type OverflowPolicy int

const (
	// OverflowDropNewest drops the message that could not be enqueued.
	OverflowDropNewest OverflowPolicy = iota
	// OverflowDropOldest removes the oldest buffered message to make room,
	// then enqueues the newest one.
	OverflowDropOldest
)

func (p OverflowPolicy) String() string {
	switch p {
	case OverflowDropNewest:
		return "drop_newest"
	case OverflowDropOldest:
		return "drop_oldest"
	default:
		return "unknown"
	}
}

// OverflowEvent is emitted whenever back pressure forces a drop. It makes
// overflow observable without forcing the publisher to block.
type OverflowEvent struct {
	SubscriberID string
	Topic        string
	Key          string
	Seq          uint64
	MessageID    string
	Policy       OverflowPolicy
	DroppedID    string // ID of the actually removed message (may equal MessageID)
}

// OverflowCallback receives overflow events; never call blocking router
// methods from it. It may be nil.
type OverflowCallback func(OverflowEvent)

// HandlerFunc receives delivered messages. Returning an error leaves a
// durable subscriber's cursor unadvanced; non-durable subscribers ignore it.
type HandlerFunc func(ctx context.Context, msg *Message) error

// subscriber is the internal, runtime representation of one subscription.
type subscriber struct {
	id         string
	filter     string
	durable    bool
	bufCap     int
	policy     OverflowPolicy
	handler    HandlerFunc
	onOverflow OverflowCallback

	ch     chan *Message
	done   chan struct{}
	closed chan struct{}

	enqMu sync.Mutex // serializes enqueue decisions vs close

	router *Router
}

func newSubscriber(r *Router, id, filter string, h HandlerFunc, o subscribeOptions) *subscriber {
	capn := o.bufferSize
	if capn <= 0 {
		capn = 16
	}
	return &subscriber{
		id:         id,
		filter:     filter,
		durable:    o.durable,
		bufCap:     capn,
		policy:     o.overflowPolicy,
		handler:    h,
		onOverflow: o.onOverflow,
		ch:         make(chan *Message, capn),
		done:       make(chan struct{}),
		closed:     make(chan struct{}),
		router:     r,
	}
}

// run is the single per-subscriber delivery goroutine. Because there is
// exactly one consumer and enqueues are FIFO, per-key order observed at
// enqueue time is preserved at delivery time. A slow handler only drains its
// own buffer; publishers use non-blocking enqueue and never wait here.
func (s *subscriber) run(ctx context.Context) {
	defer close(s.closed)
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case msg := <-s.ch:
			if msg == nil {
				continue
			}
			s.deliverOne(ctx, msg)
		}
	}
}

func (s *subscriber) deliverOne(ctx context.Context, msg *Message) {
	err := s.handler(ctx, msg)
	s.router.counters.delivered.Add(1)
	if s.durable && err == nil {
		// Best-effort durable cursor persistence. A failure leaves the
		// cursor behind, so the message is replayed after restart
		// (at-least-once) rather than lost.
		_ = s.router.advanceCursor(s.id, msg.partitionKey(), msg.Seq)
	}
}

// offer enqueues a message without blocking the publisher. It returns true
// when the message will eventually be delivered and false when it was
// dropped by the overflow policy. Exactly one overflow event is emitted on
// drop.
func (s *subscriber) offer(msg *Message) bool {
	s.enqMu.Lock()
	defer s.enqMu.Unlock()
	select {
	case <-s.done:
		return false
	default:
	}

	// Fast path: room available.
	select {
	case s.ch <- msg:
		return true
	default:
	}

	// Buffer full: apply the explicit overflow policy. Never block.
	ev := OverflowEvent{
		SubscriberID: s.id,
		Topic:        msg.Topic,
		Key:          msg.Key,
		Seq:          msg.Seq,
		MessageID:    msg.ID,
		Policy:       s.policy,
	}
	switch s.policy {
	case OverflowDropOldest:
		select {
		case dropped := <-s.ch:
			ev.DroppedID = dropped.ID
		default:
			ev.DroppedID = msg.ID
		}
		select {
		case s.ch <- msg:
		default:
			ev.DroppedID = msg.ID
		}
	default: // OverflowDropNewest
		ev.DroppedID = msg.ID
	}
	if s.onOverflow != nil {
		s.onOverflow(ev)
	}
	return false
}

func (s *subscriber) stop() {
	s.enqMu.Lock()
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	s.enqMu.Unlock()
	<-s.closed
}

// Subscription is the public handle returned by Router.Subscribe.
type Subscription struct {
	sub *subscriber
	r   *Router
}

// ID returns the stable subscriber id (used as the durable cursor key).
func (s *Subscription) ID() string { return s.sub.id }

// Filter returns the subscription topic filter.
func (s *Subscription) Filter() string { return s.sub.filter }

// Durable reports whether the subscriber persists a cursor and receives
// replay after router restart.
func (s *Subscription) Durable() bool { return s.sub.durable }

// Close unsubscribes and waits for the delivery goroutine to stop.
func (s *Subscription) Close() { s.r.unsubscribe(s.sub.id) }
