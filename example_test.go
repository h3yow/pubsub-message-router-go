package pubsub_test

import (
	"context"
	"errors"
	"fmt"

	pubsub "github.com/h3yow/pubsub-message-router-go"
)

func Example() {
	r, err := pubsub.New(pubsub.Config{})
	if err != nil {
		panic(err)
	}
	defer r.Close()

	// 订阅：通配符 + 有界缓冲 + 显式溢出策略 + 溢出可观测。
	sub, err := r.Subscribe("orders/+", 4, pubsub.PolicyDropOldest)
	if err != nil {
		panic(err)
	}
	r.OnOverflow(func(ev pubsub.OverflowEvent) {
		fmt.Printf("overflow sub=%s key=%s seq=%d\n", ev.SubscriptionID, ev.Key, ev.Seq)
	})

	ctx := context.Background()
	jsonTR := pubsub.NewJSONTransport()
	lineTR := pubsub.NewLineTransport()

	// 两种本地传输归一化后进入同一路由器。
	m1, _ := jsonTR.Decode([]byte(`{"v":1,"topic":"orders/new","key":"o","seq":1}`))
	m2, _ := lineTR.Decode([]byte("MSG/1 orders/new o 2 5\nhello"))
	for _, m := range []*pubsub.Message{m1, m2} {
		res, err := r.Publish(ctx, m)
		if err != nil {
			panic(err)
		}
		fmt.Printf("publish key=%s seq=%d verdict=%s\n", res.Key, res.Seq, res.Verdict)
	}

	// 重复消费得到明确结论，不会重复投递。
	res, _ := r.Publish(ctx, m1)
	fmt.Printf("republish seq=1 verdict=%s\n", res.Verdict)

	// 不连续序号被拒绝并给出依据。
	gap, _ := r.Publish(ctx, &pubsub.Message{Topic: "orders/new", Key: "o", Seq: 9})
	fmt.Printf("publish seq=9 verdict=%s expected=%d\n", gap.Verdict, gap.Expected)

	// 畸形报文给出可区分的拒绝原因，而非静默丢弃。
	_, err = jsonTR.Decode([]byte(`{"v":2}`))
	var reject *pubsub.RejectError
	if errors.As(err, &reject) {
		fmt.Printf("reject reason=%s\n", reject.Reason)
	}

	// 订阅者按 Key 顺序收到两条。
	for i := 0; i < 2; i++ {
		m := <-sub.Messages()
		fmt.Printf("deliver seq=%d protocol=%s payload=%q\n", m.Seq, m.Protocol, m.Payload)
	}
	// Output:
	// publish key=o seq=1 verdict=accepted
	// publish key=o seq=2 verdict=accepted
	// republish seq=1 verdict=duplicate
	// publish seq=9 verdict=sequence_gap expected=3
	// reject reason=unsupported_version
	// deliver seq=1 protocol=json/1 payload=""
	// deliver seq=2 protocol=line/1 payload="hello"
}
