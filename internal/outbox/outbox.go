// Package outbox relays messages written by the transactional outbox to a
// message broker with at-least-once delivery.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// Message is a stored outbox row.
type Message struct {
	Seq     int64
	ID      uuid.UUID
	Topic   string
	Key     string
	Payload json.RawMessage
	Headers map[string]string
}

// Source hands out unpublished messages in order. Process must call fn with
// the next batch while holding exclusive ownership of it, and mark the batch
// published only if fn returns nil.
type Source interface {
	Process(ctx context.Context, limit int, fn func(context.Context, []Message) error) (int, error)
}

// Publisher delivers messages to the broker. It returns only after every
// message has been acknowledged.
type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

// Relay moves messages from a Source to a Publisher.
type Relay struct {
	Source     Source
	Publisher  Publisher
	Batch      int
	Interval   time.Duration
	MaxBackoff time.Duration
	Log        *slog.Logger
}

// Step publishes at most one batch.
func (r *Relay) Step(ctx context.Context) (int, error) {
	batch := r.Batch
	if batch <= 0 {
		batch = 100
	}
	return r.Source.Process(ctx, batch, r.Publisher.Publish)
}

// Run drains the outbox until ctx is cancelled, backing off exponentially
// while the broker is unavailable.
func (r *Relay) Run(ctx context.Context) error {
	log := r.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	interval := r.Interval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	maxBackoff := r.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}
	backoff := interval
	for {
		n, err := r.Step(ctx)
		wait := interval
		switch {
		case err != nil && ctx.Err() == nil:
			log.ErrorContext(ctx, "outbox relay failed", "err", err, "retry_in", backoff)
			wait = backoff
			backoff = min(backoff*2, maxBackoff)
		case err == nil:
			backoff = interval
			if n > 0 {
				log.DebugContext(ctx, "outbox batch published", "count", n)
				wait = 0
			}
		}
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}
