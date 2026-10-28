package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Delivery is one consumed record.
type Delivery struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
}

// Handler processes a delivery. Returning an error stops the consumer before
// the offset is committed, so the record is redelivered after restart.
type Handler func(ctx context.Context, d Delivery) error

// Consumer is a consumer-group member that commits offsets only after the
// handler succeeded for every record in a poll.
type Consumer struct {
	client  *kgo.Client
	handler Handler
	log     *slog.Logger
	retries int
}

func NewConsumer(brokers []string, group string, topics []string, h Handler, log *slog.Logger, opts ...kgo.Opt) (*Consumer, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AllowAutoTopicCreation(),
	}
	client, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &Consumer{client: client, handler: h, log: log, retries: 5}, nil
}

// Run polls until ctx is cancelled or a record keeps failing.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.client.Close()
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if fetches.IsClientClosed() {
			return errors.New("kafka client closed")
		}
		for _, fe := range fetches.Errors() {
			c.log.WarnContext(ctx, "fetch error", "topic", fe.Topic, "partition", fe.Partition, "err", fe.Err)
		}
		var handled []*kgo.Record
		var runErr error
		fetches.EachRecord(func(r *kgo.Record) {
			if runErr != nil {
				return
			}
			if err := c.handle(ctx, r); err != nil {
				runErr = err
				return
			}
			handled = append(handled, r)
		})
		if len(handled) > 0 {
			if err := c.client.CommitRecords(ctx, handled...); err != nil && ctx.Err() == nil {
				c.log.WarnContext(ctx, "offset commit failed", "err", err)
			}
		}
		if runErr != nil {
			return runErr
		}
	}
}

func (c *Consumer) handle(ctx context.Context, r *kgo.Record) error {
	d := Delivery{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value, Headers: map[string]string{}}
	for _, h := range r.Headers {
		d.Headers[h.Key] = string(h.Value)
	}
	backoff := 100 * time.Millisecond
	var err error
	for attempt := 1; attempt <= c.retries; attempt++ {
		if err = c.handler(ctx, d); err == nil {
			return nil
		}
		c.log.WarnContext(ctx, "handler failed", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return fmt.Errorf("record %s/%d@%d: %w", r.Topic, r.Partition, r.Offset, err)
}
