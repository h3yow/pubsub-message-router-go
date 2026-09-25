package pubsub

import (
	"strconv"
	"sync"
	"sync/atomic"
)

// OverflowPolicy 是订阅者缓冲溢出时的显式策略。
type OverflowPolicy int

const (
	// PolicyDropNewest 缓冲满时丢弃最新（当前）消息，缓冲内旧消息保持顺序。
	PolicyDropNewest OverflowPolicy = iota
	// PolicyDropOldest 缓冲满时挤出最旧一条，为新消息腾位。
	PolicyDropOldest
	// PolicyFailSubscriber 缓冲满时使订阅者失败：关闭其投递通道并摘除。
	PolicyFailSubscriber
)

// OverflowEvent 记录一次溢出，供外部观测（通过 Router.OnOverflow）。
type OverflowEvent struct {
	SubscriptionID string
	Topic          string
	Key            string
	Seq            uint64
	Policy         OverflowPolicy
}

// Stats 是订阅者的可观测计数。
type Stats struct {
	Delivered     uint64
	DroppedNewest uint64
	DroppedOldest uint64
	Failed        uint64
}

// Subscription 表示一个活跃订阅，也是消费侧句柄。
type Subscription struct {
	id             string
	filter         *TopicFilter
	ch             chan *Message
	overflowPolicy OverflowPolicy
	onOverflow     func(OverflowEvent)

	delivered     atomic.Uint64
	droppedNewest atomic.Uint64
	droppedOldest atomic.Uint64
	failed        atomic.Uint64

	closeOnce sync.Once
	done      chan struct{}
}

// ID 返回订阅 ID。
func (s *Subscription) ID() string { return s.id }

// Filter 返回订阅过滤器原文。
func (s *Subscription) Filter() string { return s.filter.raw }

// Messages 返回投递通道；PolicyFailSubscriber 触发溢出时该通道会被关闭。
func (s *Subscription) Messages() <-chan *Message { return s.ch }

// Done 在订阅者因 PolicyFailSubscriber 失败而关闭后关闭。
func (s *Subscription) Done() <-chan struct{} { return s.done }

func (s *Subscription) stats() Stats {
	return Stats{
		Delivered:     s.delivered.Load(),
		DroppedNewest: s.droppedNewest.Load(),
		DroppedOldest: s.droppedOldest.Load(),
		Failed:        s.failed.Load(),
	}
}

// deliver 尝试把消息投递给订阅者，绝不阻塞发布者：
//   - 缓冲有空位：入队并计数 Delivered；
//   - 缓冲满且 PolicyDropNewest：丢弃当前消息并报告溢出；
//   - 缓冲满且 PolicyDropOldest：挤出最旧消息后重试一次；
//   - 缓冲满且 PolicyFailSubscriber：标记失败并关闭通道（只发生一次）。
//
// 返回 alive=false 表示该订阅者已失效，路由层应将其摘除。
func (s *Subscription) deliver(m *Message) (alive bool) {
	select {
	case <-s.done:
		return false
	default:
	}

	select {
	case s.ch <- m:
		s.delivered.Add(1)
		return true
	default:
	}

	switch s.overflowPolicy {
	case PolicyDropNewest:
		s.droppedNewest.Add(1)
		s.reportOverflow(m)
		return true
	case PolicyDropOldest:
		select {
		case <-s.ch:
			s.droppedOldest.Add(1)
		default:
		}
		select {
		case s.ch <- m:
			s.delivered.Add(1)
		default:
			// 极端竞争下仍无空位，按“保旧弃新”处理以免阻塞。
			s.droppedNewest.Add(1)
		}
		s.reportOverflow(m)
		return true
	case PolicyFailSubscriber:
		s.failed.Add(1)
		s.reportOverflow(m)
		s.closeOnce.Do(func() { close(s.done); close(s.ch) })
		return false
	default:
		s.droppedNewest.Add(1)
		s.reportOverflow(m)
		return true
	}
}

func (s *Subscription) reportOverflow(m *Message) {
	if s.onOverflow != nil {
		s.onOverflow(OverflowEvent{
			SubscriptionID: s.id,
			Topic:          m.Topic,
			Key:            m.Key,
			Seq:            m.Seq,
			Policy:         s.overflowPolicy,
		})
	}
}

var subSeq atomic.Uint64

func newSubscription(filter *TopicFilter, buffer int, policy OverflowPolicy, onOverflow func(OverflowEvent)) *Subscription {
	n := subSeq.Add(1)
	return &Subscription{
		id:             "sub-" + strconv.FormatUint(n, 10),
		filter:         filter,
		ch:             make(chan *Message, buffer),
		overflowPolicy: policy,
		onOverflow:     onOverflow,
		done:           make(chan struct{}),
	}
}
