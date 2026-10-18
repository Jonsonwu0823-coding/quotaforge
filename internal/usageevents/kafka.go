package usageevents

import (
	"context"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// Kafka 的主题和消费组。主题名带着事件的版本：换版本就换主题，新旧消费者不混读。
const (
	Topic = "quotaforge.usage-events.v1"
	Group = "quotaforge-usage-daily"
)

// sendTimeout 是一批事件等 Kafka 确认的上限：Kafka 不可用时，转发等这么久就放弃这一轮，下一轮再来。
const sendTimeout = 5 * time.Second

// pollMax 是消费者一次最多取的消息数。
const pollMax = 100

// Producer 把事件发到 Kafka：键是账户 id（同一个账户的事件进同一个分区、保持顺序），值是 Encode 的 JSON。
// 确认级别是 all，生产者幂等（franz-go 的默认）：客户端自己的重试不会在分区里写出重复。但这只是让重复少一些，
// 正确性靠消费者去重：发成功之后、标记之前死了照样会重发。
//
// 等待有上限（sendTimeout）：franz-go 默认不肯取消已经发给 broker、还没回音的幂等请求（取消了可能重复），broker 在
// 请求途中停掉时 ProduceSync 会一直等，转发就卡住了、积压的指标也不更新。AllowIdempotentProduceCancellation 让 ctx
// 到期时照样失败：代价是这一批下一轮重发时 Kafka 里可能多一份，由消费者去重。
// 主题不存在时自动创建（分区数和保留期由 broker 的默认值定）。
type Producer struct{ cl *kgo.Client }

// NewProducer 建一个生产者，不立即连接。
func NewProducer(brokers []string, topic string) (*Producer, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(topic), kgo.AllowAutoTopicCreation(),
		kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerLinger(0), kgo.AllowIdempotentProduceCancellation())
	if err != nil {
		return nil, err
	}
	return &Producer{cl: cl}, nil
}

// Send 同步发一批事件，都确认了才返回 nil；最多等 sendTimeout。
func (p *Producer) Send(ctx context.Context, events []ledger.UsageEvent) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sendTimeout)
	defer cancel()
	records := make([]*kgo.Record, len(events))
	for i, e := range events {
		records[i] = &kgo.Record{Key: []byte(strconv.FormatInt(e.AccountID, 10)), Value: Encode(e)}
	}
	return p.cl.ProduceSync(ctx, records...).FirstErr()
}

func (p *Producer) Close() { p.cl.Close() }

// Consumer 是消费组里的一个成员：不自动提交位点；新的消费组从最早的消息读起。
// 取到一批之后不让再均衡打断（BlockRebalanceOnPoll），提交之后才放开：分区换手之前，处理完的位点已经提交。
type Consumer struct{ cl *kgo.Client }

// NewConsumer 建一个消费者，不立即连接。
func NewConsumer(brokers []string, topic, group string) (*Consumer, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		return nil, err
	}
	return &Consumer{cl: cl}, nil
}

func (c *Consumer) Poll(ctx context.Context) ([]Record, error) {
	fetches := c.cl.PollRecords(ctx, pollMax)
	if err := ctx.Err(); err != nil {
		c.cl.AllowRebalance()
		return nil, err
	}
	var out []Record
	fetches.EachRecord(func(r *kgo.Record) {
		out = append(out, Record{Partition: r.Partition, Offset: r.Offset, Value: r.Value, raw: r})
	})
	if err := fetches.Err0(); err != nil && len(out) == 0 {
		c.cl.AllowRebalance()
		return nil, err
	}
	return out, nil
}

func (c *Consumer) Commit(ctx context.Context, done []Record) error {
	defer c.cl.AllowRebalance()
	records := make([]*kgo.Record, len(done))
	for i, r := range done {
		records[i] = r.raw.(*kgo.Record)
	}
	if len(records) == 0 {
		return nil
	}
	return c.cl.CommitRecords(ctx, records...)
}

// Close 离开消费组、关掉连接。
func (c *Consumer) Close() { c.cl.Close() }
