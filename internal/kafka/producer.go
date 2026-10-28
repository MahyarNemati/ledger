// Package kafka adapts franz-go to the outbox publisher and to consumers.
package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/MahyarNemati/ledger/internal/outbox"
)

// Producer publishes outbox messages. franz-go's idempotent producer is on by
// default, so broker-side retries do not duplicate records; duplicates from
// relay restarts are handled by consumer inboxes.
type Producer struct {
	client *kgo.Client
}

var _ outbox.Publisher = (*Producer)(nil)

// NewProducer connects to brokers. Records with the same key land on the same
// partition, which preserves per-stream ordering.
func NewProducer(brokers []string, opts ...kgo.Opt) (*Producer, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.NoCompression()),
		kgo.AllowAutoTopicCreation(),
	}
	client, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Producer{client: client}, nil
}

func (p *Producer) Publish(ctx context.Context, msgs []outbox.Message) error {
	records := make([]*kgo.Record, len(msgs))
	for i, m := range msgs {
		r := &kgo.Record{Topic: m.Topic, Key: []byte(m.Key), Value: m.Payload}
		for k, v := range m.Headers {
			r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		records[i] = r
	}
	return p.client.ProduceSync(ctx, records...).FirstErr()
}

// Ping checks broker connectivity.
func (p *Producer) Ping(ctx context.Context) error { return p.client.Ping(ctx) }

func (p *Producer) Close() { p.client.Close() }
