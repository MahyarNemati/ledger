//go:build integration

package kafka_test

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/MahyarNemati/ledger/internal/kafka"
	"github.com/MahyarNemati/ledger/internal/outbox"
)

func brokers(t *testing.T) []string {
	t.Helper()
	if v := os.Getenv("LEDGER_TEST_KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	c, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v24.2.7", redpanda.WithAutoCreateTopics())
	if err != nil {
		t.Fatalf("redpanda: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	addr, err := c.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return []string{addr}
}

func TestPublishAndConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	seeds := brokers(t)
	topic := "ledger-it-" + uuid.NewString()[:8]

	p, err := kafka.NewProducer(seeds)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var msgs []outbox.Message
	for i := 0; i < 5; i++ {
		id := uuid.New()
		msgs = append(msgs, outbox.Message{ID: id, Topic: topic, Key: "account-1", Payload: []byte(`{"n":` + string(rune('0'+i)) + `}`),
			Headers: map[string]string{"message-id": id.String()}})
	}
	if err := p.Publish(ctx, msgs); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var got []kafka.Delivery
	runCtx, stop := context.WithCancel(ctx)
	c, err := kafka.NewConsumer(seeds, "it-"+topic, []string{topic}, func(_ context.Context, d kafka.Delivery) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, d)
		if len(got) == len(msgs) {
			stop()
		}
		return nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Run(runCtx); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(msgs) {
		t.Fatalf("consumed %d of %d", len(got), len(msgs))
	}
	for i, d := range got {
		if d.Headers["message-id"] != msgs[i].ID.String() {
			t.Fatalf("record %d out of order", i)
		}
	}
}
