package pubsub

import (
	"context"
	"fmt"
	"sync"
)

// PublishVerdict 是发布请求的明确结论。
type PublishVerdict string

const (
	// VerdictAccepted 顺序连续，已接受并持久化、进入投递。
	VerdictAccepted PublishVerdict = "accepted"
	// VerdictDuplicate 重复序号：该 Key 的此序号（或更高序号）此前已接受，
	// 不重复投递，也不再次持久化。
	VerdictDuplicate PublishVerdict = "duplicate"
	// VerdictGap 序号超前产生空洞：seq > 期望序号，拒绝以要求先补齐中间序号。
	VerdictGap PublishVerdict = "sequence_gap"
)

// PublishResult 携带发布结论与判定依据，便于日志解释。
type PublishResult struct {
	Verdict PublishVerdict
	Key     string
	Seq     uint64
	// Expected 为该 Key 的下一个期望序号。
	Expected uint64
	// LastAccepted 为该 Key 已接受的最大序号（0 表示尚无消息）。
	LastAccepted uint64
}

func (r PublishResult) String() string {
	return fmt.Sprintf("verdict=%s key=%s seq=%d expected=%d lastAccepted=%d",
		r.Verdict, r.Key, r.Seq, r.Expected, r.LastAccepted)
}

// Config 为 Router 配置。
type Config struct {
	// JournalPath 非空时启用持久化日志，重启后恢复顺序水位。
	JournalPath string
}

// Router 是本地发布订阅路由器。
type Router struct {
	mu sync.Mutex
	// expected 记录每个顺序键下一个期望序号（= 已接受最大序号 + 1）。
	expected    map[string]uint64
	keyLocks    map[string]*sync.Mutex
	subscribers map[string]*Subscription
	journal     *Journal
	overflow    func(OverflowEvent)
	closed      bool
}

// New 创建路由器；配置持久化时执行崩溃恢复，重建每个 Key 的高水位。
// 恢复阶段若发现日志尾部损坏，返回包装后的 *CorruptionError（可用
// errors.As 识别），但路由器仍可正常使用（损坏尾部已被安全截断）。
func New(cfg Config) (*Router, error) {
	r := &Router{
		expected:    make(map[string]uint64),
		keyLocks:    make(map[string]*sync.Mutex),
		subscribers: make(map[string]*Subscription),
	}
	if cfg.JournalPath != "" {
		j, err := OpenJournal(cfg.JournalPath)
		if err != nil {
			if _, ok := err.(*CorruptionError); !ok {
				return nil, fmt.Errorf("open journal: %w", err)
			}
			// 损坏已截断，记录错误但继续启动；水位来自有效前缀。
			r.journal = j
			r.rebuildWatermarks()
			return r, err
		}
		r.journal = j
		r.rebuildWatermarks()
	}
	return r, nil
}

func (r *Router) rebuildWatermarks() {
	if r.journal == nil {
		return
	}
	// 高水位已由 Journal.recover 建立；这里同步到路由器视图。
	// 通过 Replay 遍历可同时覆盖历史消息并校验。
	_ = r.journal.Replay(func(m *Message) error {
		if m.Seq >= r.expected[m.Key] {
			r.expected[m.Key] = m.Seq + 1
		}
		return nil
	})
}

// Subscribe 建立订阅。buffer 必须 >0，溢出按 policy 显式处理。
func (r *Router) Subscribe(filter string, buffer int, policy OverflowPolicy) (*Subscription, error) {
	if buffer <= 0 {
		return nil, fmt.Errorf("buffer must be > 0, got %d", buffer)
	}
	tf, err := ParseFilter(filter)
	if err != nil {
		return nil, err
	}
	sub := newSubscription(tf, buffer, policy, func(ev OverflowEvent) {
		r.mu.Lock()
		fn := r.overflow
		r.mu.Unlock()
		if fn != nil {
			fn(ev)
		}
	})

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("router closed")
	}
	r.subscribers[sub.id] = sub
	return sub, nil
}

// Unsubscribe 摘除订阅并关闭其通道。
func (r *Router) Unsubscribe(sub *Subscription) {
	r.mu.Lock()
	delete(r.subscribers, sub.id)
	r.mu.Unlock()
	sub.closeOnce.Do(func() { close(sub.done); close(sub.ch) })
}

// Publish 对归一化消息执行顺序检查、持久化与路由。
//
// 判定依据（按 Key 维度，线程安全）：
//   - 该 Key 下期望序号为 expected=max+1（首条为 1）；
//   - seq < expected：重复/回退 -> VerdictDuplicate，不投递、不持久化；
//   - seq == expected：接受 -> 持久化（若启用）后路由，水位推进；
//   - seq > expected：不连续空洞 -> VerdictGap，拒绝，水位不变。
//
// 同一 Key 的发布经由每 Key 互斥锁串行化（含 fsync），保证落盘与
// 路由顺序严格等于接受顺序；不同 Key 可并发。
func (r *Router) Publish(ctx context.Context, m *Message) (PublishResult, error) {
	if m == nil {
		return PublishResult{}, fmt.Errorf("nil message")
	}
	if m.Topic == "" || m.Key == "" || m.Seq == 0 {
		return PublishResult{}, &RejectError{Reason: RejectMissingField,
			Detail: "router requires non-empty topic/key and seq>=1"}
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return PublishResult{}, fmt.Errorf("router closed")
	}
	kl, ok := r.keyLocks[m.Key]
	if !ok {
		kl = &sync.Mutex{}
		r.keyLocks[m.Key] = kl
	}
	r.mu.Unlock()

	kl.Lock()
	defer kl.Unlock()

	r.mu.Lock()
	expected := r.expected[m.Key]
	if expected == 0 {
		expected = 1
	}

	switch {
	case m.Seq < expected:
		res := PublishResult{
			Verdict:      VerdictDuplicate,
			Key:          m.Key,
			Seq:          m.Seq,
			Expected:     expected,
			LastAccepted: expected - 1,
		}
		r.mu.Unlock()
		return res, nil
	case m.Seq > expected:
		res := PublishResult{
			Verdict:      VerdictGap,
			Key:          m.Key,
			Seq:          m.Seq,
			Expected:     expected,
			LastAccepted: expected - 1,
		}
		r.mu.Unlock()
		return res, nil
	}
	journal := r.journal
	r.mu.Unlock()

	// 持久化在锁外（但受每 Key 锁保护）：同 Key 严格顺序落盘，
	// 崩溃时不会出现已接受消息丢失，或水位越过已落盘记录。
	if journal != nil {
		if err := journal.Append(m); err != nil {
			return PublishResult{}, fmt.Errorf("journal append: %w", err)
		}
	}

	r.mu.Lock()
	r.expected[m.Key] = expected + 1
	subs := make([]*Subscription, 0, len(r.subscribers))
	for _, s := range r.subscribers {
		if s.filter.Match(m.Topic) {
			subs = append(subs, s)
		}
	}
	r.mu.Unlock()

	cancelled := ctx.Err()

	dead := make([]string, 0)
	for _, s := range subs {
		if !s.deliver(m) {
			dead = append(dead, s.id)
		}
	}
	if len(dead) > 0 {
		r.mu.Lock()
		for _, id := range dead {
			delete(r.subscribers, id)
		}
		r.mu.Unlock()
	}

	return PublishResult{
		Verdict:      VerdictAccepted,
		Key:          m.Key,
		Seq:          m.Seq,
		Expected:     expected + 1,
		LastAccepted: m.Seq,
	}, cancelled
}

// ReplayTo 把已持久化消息中匹配订阅过滤器的部分，按提交顺序投递到
// 指定订阅者。它用于进程重启后向“新建立”的订阅者补投崩溃前的消息，
// 结合启动时恢复的水位，实现重启后不丢、不重。
//
// 投递同样遵守订阅者的有界缓冲与溢出策略。
func (r *Router) ReplayTo(sub *Subscription) (int, error) {
	if r.journal == nil {
		return 0, nil
	}
	delivered := 0
	err := r.journal.Replay(func(m *Message) error {
		if sub.filter.Match(m.Topic) {
			if !sub.deliver(m) {
				return errSubscriberFailed
			}
			delivered++
		}
		return nil
	})
	return delivered, err
}

// errSubscriberFailed 表示回放目标订阅者已失败关闭。
var errSubscriberFailed = fmt.Errorf("subscriber failed during replay")

// OnOverflow 注册溢出观测回调（每次 DropNewest/DropOldest/Fail 触发）。
func (r *Router) OnOverflow(fn func(OverflowEvent)) {
	r.mu.Lock()
	r.overflow = fn
	r.mu.Unlock()
}

// Stats 返回某订阅者的计数快照。
func (r *Router) Stats(subID string) (Stats, bool) {
	r.mu.Lock()
	s, ok := r.subscribers[subID]
	r.mu.Unlock()
	if !ok {
		return Stats{}, false
	}
	return s.stats(), true
}

// Close 关闭路由器、摘除并关闭全部订阅、关闭持久化日志。
func (r *Router) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	subs := r.subscribers
	r.subscribers = make(map[string]*Subscription)
	j := r.journal
	r.mu.Unlock()

	for _, s := range subs {
		s.closeOnce.Do(func() { close(s.done); close(s.ch) })
	}
	if j != nil {
		return j.Close()
	}
	return nil
}
