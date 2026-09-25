# 本地发布订阅消息路由组件（Go）

把来自**不同本地传输**的消息归一到统一模型，在订阅路由、重复消费、序号回退与并发
压力下给出**一致且可解释**的投递结论：任何不接受都有稳定、可区分的原因码，绝不静默
丢弃或静默跳过。

- 纯 Go，无第三方依赖；模块路径 `github.com/h3yow/pubsub-message-router-go`
- 两个包：根包 `router`（路由/顺序/去重/背压/持久化），子包 `transport`（传输适配）

---

## 1. 统一消息模型

所有传输都解码为 `router.Message`：

| 字段 | 含义 | 校验 |
|---|---|---|
| `version` | 归一化模型版本，当前仅支持 `"1.0"` | 其他值 → `unsupported_version` |
| `id` | 生产者生成的全局唯一消息 ID，幂等去重依据 | 空 → `missing_id` |
| `topic` | 具体主题（非通配符） | 空 → `missing_topic` |
| `key` | 可选分区键；为空时分区键即 topic | — |
| `seq` | 生产者在该分区键内分配的、无空洞、从 1 严格递增的序号 | `0` → `zero_seq` |
| `payload` | 不透明字节载荷，可为空 | — |

传输层线格式（JSON line / native 共用）为单帧 JSON：
`{"v":"1.0","id","topic","key?","seq","payload?(base64)","crc32?"}`，
`crc32`（IEEE）存在时按解码后载荷校验。

### 两种本地传输

1. **native（进程内直调）** `transport.NativeTransport`：调用方直接送入编码帧，
   与线传输走完全相同的校验/发布管线。
2. **jsonline（本地流）** `transport.JSONLineTransport`：从文件、管道、unix socket
   等任意 `io.Reader` 逐行读取帧，逐帧给出判定报告。

### 可区分的拒绝原因

传输层 `transport.RejectCode`（坏报文不静默丢弃）：

| code | 触发条件 |
|---|---|
| `malformed_json` | 帧不是合法 JSON |
| `unsupported_protocol_version` | 帧 `v` 不是 `"1.0"` |
| `missing_field` | `id`/`topic` 缺失或 `seq<1` |
| `bad_field` | payload 不是合法 base64 |
| `checksum_mismatch` | crc32 校验失败（报文损坏） |
| `frame_too_large` | 行长度超过 16 MiB |
| `empty_frame` | 空输入 |
| `router_rejected` | 帧合法但被路由层拒绝，详情见 `Router.Reason` |

路由层 `router.RejectReason`：`duplicate`、`seq_rollback`、`seq_gap`、
`unsupported_version`、`missing_id`、`missing_topic`、`zero_seq`、`router_closed`。

---

## 2. 订阅、路由与顺序规则

### 主题匹配（`MatchTopic`，MQTT 风格，`/` 分段）

- `+` 匹配恰好一个分段：`a/+` 匹配 `a/b`，不匹配 `a/b/c`
- `#` 只允许出现在末段，匹配零或多个尾部分段：`a/#` 匹配 `a`、`a/b`、`a/b/c`
- 其余按字面大小写敏感比较；`#` 出现在非末段的过滤器视为非法、不匹配

### 顺序与分区

顺序保证的范围是**分区键** `(topic, key)`（`key` 为空时即 topic）：

- 路由器内部按分区键做 32 路哈希分片，分片锁串行化同键的判定与提交；
- 每个订阅者只有**一个投递 goroutine**、一个有界 FIFO 缓冲，因此入队顺序即投递
  顺序，同键严格有序；不同分区键之间不保证全局顺序（设计上相互独立）。

### 序号判定（明确、可区分，不静默跳过/重复投递）

设分区键当前期望序号为 `nextSeq`（初始 1）：

| 输入 | 结论 | 处理 |
|---|---|---|
| `id` 已见过（窗口 4096） | **`duplicate`** | 拒绝，绝不重复投递 |
| `seq < nextSeq` 且 `id` 未见 | **`seq_rollback`**（序号回退） | 拒绝，绝不投递 |
| `seq > nextSeq` | **`seq_gap`**（不连续） | 拒绝并指明期望值，而不是缓存后静默跳过 |
| `seq == nextSeq` 且 `id` 未见 | **接受** | 先持久化再 fan-out |

缺口不会推进前沿：拒绝 `seq=5` 后，`seq=3` 仍可正常接受（当期望为 3 时）。

---

## 3. 背压策略

- 每个订阅者拥有独立的**有界缓冲**（`WithBufferSize`，默认 16）和独立投递协程；
- 发布者对每个订阅者使用 **non-blocking enqueue**：慢订阅者**不会阻塞发布者，也不会
  阻塞其他订阅者**；
- 溢出策略在订阅时显式声明（`WithOverflowPolicy`），每次溢出都通过
  `WithOverflowCallback` 回调一个 `OverflowEvent`（订阅者、主题、序号、丢弃的消息 ID、
  策略）并累加 `Stats.DroppedBackpressure`，**可观测**：
  - `OverflowDropNewest`（默认）：丢弃新消息，保留已有 FIFO；
  - `OverflowDropOldest`：从队首移除最旧消息以容纳最新消息（保证最新值可达）。

> 注意：溢出丢弃是一种**显式的数据取舍**，事件与计数器让它可被监控，而非悄悄发生。

---

## 4. 持久化与重启（不丢、不乱、不重复的边界）

可选追加式日志 `router.Journal`（长度前缀二进制帧，魔数 `PJ`，默认每帧 `fsync`，
`WithoutFsync()` 可关闭）：

- 接受消息先写日志再 fan-out；记录类型为「消息」与「游标」；
- **重启时重放**：重建每个分区键的序号前沿与最近 ID 表，因此重启后
  `duplicate`/`seq_rollback`/`seq_gap` 判定依然成立，且后续消息按原顺序衔接，不乱序；
- **持久订阅者**（`WithDurable("稳定ID")`）的游标按「订阅者 × 分区键」记录；重启后
  用同一 ID 重新订阅，会把未确认（`seq > ackSeq`）的保留消息按序回放，语义为
  **at-least-once**：崩溃只会导致重放，不会丢失；消费方应以消息 `id` 做业务幂等。
- 回放保留量由 `Config.RetainPerKey` 限定（每键保留最近 N 条）。
- 日志损坏（截断帧、坏魔数、坏 JSON、未知记录类型）在启动重放时返回
  `ErrJournalCorrupt`，**报错而不是静默截断**。
- 不配置 `Journal`（或 `InMemoryJournal()`）时为纯内存路由，重启保证不适用。

「不重复」的精确含义：路由层对任何消息**绝不向同一活订阅者重复投递**；重启场景下，
未确认消息按 at-least-once 重放，由消息 ID 支撑消费端幂等——这是持久消息系统的标准
取舍，并在 API 与文档中显式说明。

### 并发发布的一致性

多发布者并发发布同一分区键时：恰好一个生产者赢得该序号（接受），其余获得
`seq_gap`/`seq_rollback`/`duplicate` 的明确结论（可在业务侧重试）。无论如何调度，
被接受的集合恰好是 `seq=1..N` 各一次，不丢不重（有测试在 `-race` 下断言）。

---

## 5. 可观测性

`Router.Snapshot()` 返回原子计数快照：

`published / accepted / rejected / duplicates / seq_rollbacks / seq_gaps /
delivered / dropped_backpressure / subscribers_live / replayed`

配置 `Config.Logger` 后，每次判定都打印一行，包含 **主题、序号、消息 ID 与判定依据**。

---

## 6. 兼容范围

- Go 版本：见 `go.mod`（`go 1.25.0`），在 Go 1.26 上验证。
- 线协议版本：仅 `"1.0"`；模型版本：仅 `router.Version = "1.0"`。
- 本地传输：进程内直调、换行分隔 JSON 流（文件/管道/unix socket 等）。
  其他传输可通过实现 `transport.Ingester` 或直接调用 `Router.Publish` 接入。

## 7. 验证方法

```bash
go build ./...
go vet ./...
# 普通单测
go test ./...
# 竞态检测 + 重复执行（并发发布/重复消费/畸形报文/慢订阅者/重启回放均覆盖）
go test -race -count=3 ./...
# 查看含主题、序号与判定依据的单测日志
go test -v -run 'Duplicate|Backpressure|Concurrent|Malformed|Replay' ./...
```

测试覆盖：

- `TestConcurrentPublishSameKeyNoLossNoDup`：8 发布者 × 200 序号并发，接受集合恰为
  1..200 各一次（竞态检测）；
- `TestPerKeyOrdering`：100 条同键消息投递严格升序；
- `TestDuplicateAndSeqConclusions`：重复 / 序号回退 / 缺口三种结论可区分且计数器分列；
- `TestSlowSubscriberBackpressure`：慢订阅者不阻塞发布者与另一快订阅者，溢出事件 +
  计数器可观测；`TestDropOldestKeepsNewest` 验证丢旧保新；
- `TestMalformedFramesDistinguishable` / `TestJSONLineStreamMixed`：坏 JSON、坏版本、
  缺字段、坏 base64、crc 损坏、超长帧各有独立 code；
- `TestJournalRestartFrontierAndNoLoss` / `TestDurableSubscriberReplayAfterRestart` /
  `TestJournalCorruptionReportedNotSkipped`：重启恢复、游标回放、损坏报错。

## 8. 快速上手

```go
r, _ := router.New(router.Config{})
defer r.Close()

sub, _ := r.Subscribe("sensor/+/temp", handler,
    router.WithBufferSize(128),
    router.WithOverflowPolicy(router.OverflowDropNewest),
    router.WithOverflowCallback(func(e router.OverflowEvent) { log.Println(e) }))
defer sub.Close()

res := r.Publish(ctx, &router.Message{
    Version: router.Version, ID: "uuid-1", Topic: "sensor/a/temp", Seq: 1,
})
// res.Accepted / res.Reason / res.Detail 给出可解释结论
```
