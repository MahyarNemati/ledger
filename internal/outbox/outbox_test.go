package outbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type flakyPublisher struct {
	mu       sync.Mutex
	failures int
	got      []Message
	calls    int
}

func (p *flakyPublisher) Publish(_ context.Context, msgs []Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.failures > 0 {
		p.failures--
		// A partial send before failing is what makes delivery at-least-once.
		p.got = append(p.got, msgs[0])
		return errors.New("broker unavailable")
	}
	p.got = append(p.got, msgs...)
	return nil
}

func msgs(n int) []Message {
	out := make([]Message, n)
	for i := range out {
		out[i] = Message{ID: uuid.New(), Topic: "ledger.events", Key: "k"}
	}
	return out
}

func TestRelayPublishesInOrderAndRetries(t *testing.T) {
	src := &Memory{}
	src.Add(msgs(7)...)
	pub := &flakyPublisher{failures: 2}
	r := &Relay{Source: src, Publisher: pub, Batch: 3}
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := r.Step(ctx); err == nil {
			t.Fatal("expected failure")
		}
	}
	if src.Pending() != 7 {
		t.Fatalf("failed batches must stay pending, %d left", src.Pending())
	}
	for {
		n, err := r.Step(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	published := src.Published()
	if len(published) != 7 {
		t.Fatalf("published %d, want 7", len(published))
	}
	for i := 1; i < len(published); i++ {
		if published[i].Seq <= published[i-1].Seq {
			t.Fatal("out of order")
		}
	}
	// Two failed attempts each leaked one message, so the broker saw duplicates.
	if len(pub.got) != 9 {
		t.Fatalf("broker received %d messages, want 9 (7 + 2 duplicates)", len(pub.got))
	}
}

func TestRelayRunStopsOnCancel(t *testing.T) {
	src := &Memory{}
	src.Add(msgs(3)...)
	pub := &flakyPublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- (&Relay{Source: src, Publisher: pub, Interval: time.Millisecond}).Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for src.Pending() > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if src.Pending() != 0 {
		t.Fatal("relay did not drain the outbox")
	}
}
