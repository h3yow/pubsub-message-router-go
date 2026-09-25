package pubsub

// Message 是两种本地传输归一化后的统一消息模型。
//
// 无论输入来自哪种本地传输（JSON 报文或行文本报文），适配器都必须
// 产出或拒绝一个 Message；被拒绝的报文以带 RejectReason 的错误返回，
// 绝不会被静默丢弃。
type Message struct {
	// Protocol 标识消息来源的传输协议（如 "json/1"、"line/1"）。
	Protocol string
	// Topic 是用于订阅匹配的分层主题，以 "/" 分层，例如 "orders/new/eu"。
	Topic string
	// Key 是顺序键；同一 Key 内严格按 Seq 连续递增顺序投递。
	Key string
	// Seq 是 Key 内从 1 开始、连续递增的单调序号。
	Seq uint64
	// Payload 为业务负载字节。
	Payload []byte
}

// RoutingKey 返回顺序与去重维度使用的复合键。
// 顺序按 Key 维护；Topic 仅决定路由给哪些订阅者。
func (m *Message) RoutingKey() string {
	return m.Key
}
