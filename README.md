# 本地发布订阅消息路由组件（Go）

把来自**不同本地传输**的消息归一化为统一消息模型，在**按主题订阅路由（含通配符）**、
**重复消费/序号回退**、**并发发布**、**慢订阅者**与**进程重启**等条件下，
给出**一致、确定、可解释**的投递结论。任何被拒绝的输入都带**可区分的拒绝原因**，
绝不静默丢弃。

模块路径：`github.com/h3yow/pubsub-message-router-go`（包名 `pubsub`，无第三方依赖）。

---

## 1. 统一消息模型

两种传输解码后都得到同一个 `Message`：

| 字段       | 含义                                                                 |
|------------|----------------------------------------------------------------------|
| `Protocol` | 来源协议标识：`json/1` 或 `line/1`                                   |
| `Topic`    | 分层主题，`/` 分隔，如 `orders/new/eu`，仅用于订阅匹配               |
| `Key`      | **顺序键**。顺序、去重、水位都以 `Key` 为维度，与 `Topic` 解耦       |
| `Seq`      | `Key` 内从 **1** 开始、**连续递增**的单调序号                        |
| `Payload`  | 业务负载字节（JSON 传输用 base64；行传输按长度原样承载，可含换行）   |

### 支持的本地传输与兼容范围

1. **JSON 数据报文 `json/1`**
   ```json
   {"v":1,"topic":"orders/new","key":"orders","seq":1,"payload":"aGVsbG8="}
   ```
   `payload` 为原始字节的 base64 编码。**仅支持 `v=1`**。

2. **行文本长度分隔协议 `line/1`**
   ```
   MSG/1 <topic> <key> <seq> <payload-length>\n
   <恰好 payload-length 字节的载荷，可包含任意字节含换行>
   ```
   **仅支持 `MSG/1`**。载荷长度与声明不符即视为损坏。

新增传输只需实现：

```go
type Transport interface {
    Name() string
    Decode(frame []byte) (*Message, error)
}
```

### 输入拒绝原因（可区分，不静默丢弃）

解码失败统一返回 `*RejectError`，其 `Reason` 取值：

| `RejectReason`         | 触发条件                                   |
|------------------------|--------------------------------------------|
| `malformed_frame`      | 报文结构损坏、非法 JSON、长度不符、多报文  |
| `unsupported_version`  | 协议名/版本缺失或不受支持（如 `v=2`、`MSG/9`） |
| `missing_field`        | `topic` / `key` / 头等必填字段缺失         |
| `bad_topic`            | 主题含空层级（如 `a//b`）、过滤器 `#` 位置非法 |
| `bad_sequence`         | `seq` 无法解析或为 0                       |

用 `errors.As(err, &reject)` 即可按原因分流处理。

---

## 2. 主题订阅与路由

- 主题以 `/` 分层；订阅过滤器支持 MQTT 风格通配符：
  - `+`：匹配恰好一个层级（`a/+/c` 匹配 `a/b/c`）；
  - `#`：匹配零到多个层级且只能在末尾（`a/#` 匹配 `a`、`a/b`、`a/b/c`）。
- 发布的每条消息会路由给**所有匹配的订阅者**，每个订阅者独立判定背压。
- `Subscribe(filter, buffer, policy)` 创建订阅；`Unsubscribe` 摘除。

---

## 3. 投递与顺序规则（含重复消费与序号回退）

顺序与去重的维度是 **`Key`**，不是 `Topic`。路由器为每个 `Key` 维护
**期望序号 `expected`（= 已接受最大序号 + 1，首条为 1）**，并由
**每 Key 互斥锁**串行化该 Key 的“判定 → 持久化 → 推进水位 → 路由”。

对 `Publish(m)` 的判定结论 `PublishResult.Verdict`：

| 条件               | 结论             | 处理                                                     |
|--------------------|------------------|----------------------------------------------------------|
| `seq == expected`  | `accepted`       | 接受：持久化（启用日志时 fsync）后路由，水位推进          |
| `seq < expected`   | `duplicate`      | **重复/回退**：不投递、不重复持久化，返回期望水位作依据   |
| `seq > expected`   | `sequence_gap`   | **不连续空洞**：拒绝，要求先补齐 `expected..seq-1`       |

因此：

- **同一 `Key` 严格有序**：并发发布者即使提交顺序乱序，超前的消息也会得到
  `sequence_gap`；发布方等待前序补齐并重试后，最终投递顺序严格等于序号顺序。
- **重复消费/序号回退不静默跳过也不重复投递**：一律返回 `duplicate`，
  订阅者对每个 `(Key, Seq)` 恰好收到一次。
- **不同 `Key` 互相独立、可并发**：一个慢 Key 不影响其他 Key。
- 判定依据随结果返回：`Key/Seq/Expected/LastAccepted`，测试日志据此解释。

> 说明：本组件不替发布者“自动补洞”，而是把空洞显式判为 `sequence_gap`，
> 由发送方按期望序号补齐。这使得“乱序到达”这一事实可被明确观测，而不是
> 被缓冲层悄悄吞掉。

---

## 4. 背压策略（显式且可观测）

每个订阅者持有一个**有界缓冲**（`buffer > 0`）。`Publish` 对订阅者的
投递始终**非阻塞**：缓冲满时按订阅时声明的策略处理，**慢订阅者不会阻塞
发布者或其他订阅者**。

| 策略                     | 满时行为                                       | 计数                |
|--------------------------|------------------------------------------------|---------------------|
| `PolicyDropNewest`（默认）| 丢弃当前最新消息，保留缓冲内旧消息             | `DroppedNewest`     |
| `PolicyDropOldest`       | 挤出最旧一条，为新消息腾位，保留最新窗口       | `DroppedOldest`     |
| `PolicyFailSubscriber`   | 使订阅者失败：关闭其通道并从路由器摘除         | `Failed`，通道关闭  |

- `Router.OnOverflow(func(OverflowEvent))` 注册回调，每次溢出都产生一条
  含 `SubscriptionID/Topic/Key/Seq/Policy` 的事件。
- `Router.Stats(subID)` 返回 `Delivered / DroppedNewest / DroppedOldest / Failed`。

> 溢出是一种**有意的、按订阅者隔离的取舍**：在持续超载时保证系统整体不被
> 拖垮，同时把“谁、因为什么、丢了哪条”暴露出来。若业务不允许丢弃，应让
> 消费侧足够快并配合持久化日志与 `ReplayTo` 补投，而不是调大到无界缓冲。

---

## 5. 持久化与进程重启（不丢、不重、不乱序）

`Config{JournalPath: "..."}` 启用追加式持久化日志：

- 记录格式：`magic "PSMJ" + version + seq + 长度前缀字段(protocol/topic/key)
  + payload + CRC32`。每条 `accepted` 消息在路由前 `fsync` 落盘；
  `duplicate` 与 `sequence_gap` 不落盘。
- **重启恢复水位**：打开日志时顺序校验全部记录，重建每个 `Key` 的高水位。
  重启后重放旧序号得到 `duplicate`，从下一个期望序号续接。
- **补投新订阅者**：`Router.ReplayTo(sub)` 按提交顺序把匹配的历史消息投递给
  重启后新建的订阅者，配合恢复水位实现“不丢、不重”。
- **损坏可检测**：尾部截断/CRC 错误会得到 `*CorruptionError`（含偏移与原因），
  组件自动截断到最后一条有效记录后继续可用；已提交前缀的水位不丢失。
  中间记录的损坏不会被静默跳过。

并发模型：全局互斥锁仅保护订阅者集合与水位映射的短临界区；每条 `Key`
有独立顺序锁，**不同 Key 的持久化可并发，同一 Key 严格顺序**。

---

## 6. 快速上手

```go
r, _ := pubsub.New(pubsub.Config{JournalPath: "./router.bin"})
defer r.Close()

sub, _ := r.Subscribe("orders/+", 64, pubsub.PolicyDropNewest)
r.OnOverflow(func(ev pubsub.OverflowEvent) {
    log.Printf("overflow %+v", ev)
})

tr := pubsub.NewJSONTransport()
m, err := tr.Decode(frame)          // 畸形输入得到带 RejectReason 的错误
if err != nil { /* errors.As 分流 */ }

res, err := r.Publish(ctx, m)       // accepted / duplicate / sequence_gap
for msg := range sub.Messages() {
    // 同一 Key 严格按 Seq 顺序到达
    _ = msg
}

// 重启后：新订阅者可重放历史
n, err := r.ReplayTo(sub)
```

---

## 7. 验证方法

全部测试在 **竞态检测** 下运行：

```bash
go test -race -count=1 ./...
```

覆盖场景：

| 测试                                   | 覆盖内容                                                       |
|----------------------------------------|----------------------------------------------------------------|
| `TestTopicWildcardMatch`               | `+`/`#` 通配符匹配与非法过滤器                                |
| `TestJSONTransportRejects/Accepts`     | JSON 畸形/版本/缺字段/坏主题/坏序号的可区分拒绝               |
| `TestLineTransportRejects/Accepts`     | 行协议同类拒绝、载荷含换行、长度不符                          |
| `TestPublishSequenceVerdicts`          | accepted/duplicate/sequence_gap 的判定与日志依据              |
| `TestPerKeyOrderingAndRouting`         | 多订阅者按 Key 有序、非匹配订阅者零投递                        |
| `TestConcurrentPublishersPerKeyOrder`  | 8 发布者 × 8 Key 并发、乱序重试、重复重发：不丢/不重/不乱序   |
| `TestSlowSubscriberDoesNotBlockOthers` | 慢订阅者不阻塞发布者/快订阅者，DropNewest 计数与溢出事件      |
| `TestOverflowPolicyDropOldest`         | 挤旧保新窗口                                                   |
| `TestOverflowPolicyFailSubscriber`     | 溢出失败、通道关闭、自动摘除、后续发布正常                     |
| `TestRestartRecoversWatermarks`        | 重启水位恢复、duplicate、续接、`ReplayTo` 不丢不重            |
| `TestJournalCorruptionDetected`        | 尾部损坏检测 + 安全截断 + 前缀水位完好                         |
| `TestMixedTransportsUnifiedSemantics`  | 两种传输混合进入同一路由器，统一顺序域与可区分拒绝            |
| `Example`                              | 可运行的端到端用法（`go test -run Example`）                  |

测试日志会打印**主题、Key、序号、期望水位与判定依据**，失败原因可区分。
