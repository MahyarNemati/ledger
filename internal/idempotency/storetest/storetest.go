// Package storetest is a conformance suite for idempotency.Store implementations.
package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/MahyarNemati/ledger/internal/idempotency"
)

// Run executes the suite against stores produced by newStore.
func Run(t *testing.T, newStore func(t *testing.T) idempotency.Store) {
	ctx := context.Background()
	h1 := idempotency.Hash("POST", "/v1/transfers", []byte(`{"amount": 5}`))
	h2 := idempotency.Hash("POST", "/v1/transfers", []byte(`{"amount": 6}`))

	t.Run("FirstRequestExecutesThenReplays", func(t *testing.T) {
		s, key := newStore(t), uuid.NewString()
		rec, err := s.Begin(ctx, "transfers", key, h1)
		if err != nil || rec != nil {
			t.Fatalf("first Begin = %v, %v", rec, err)
		}
		if _, err := s.Begin(ctx, "transfers", key, h1); !errors.Is(err, idempotency.ErrInProgress) {
			t.Fatalf("concurrent Begin: want in progress, got %v", err)
		}
		if err := s.Complete(ctx, "transfers", key, idempotency.Record{StatusCode: 201, Body: []byte(`{"id":"x"}`)}); err != nil {
			t.Fatal(err)
		}
		rec, err = s.Begin(ctx, "transfers", key, h1)
		if err != nil || rec == nil || rec.StatusCode != 201 || string(rec.Body) != `{"id":"x"}` {
			t.Fatalf("replay = %+v, %v", rec, err)
		}
	})

	t.Run("DifferentBodyIsRejected", func(t *testing.T) {
		s, key := newStore(t), uuid.NewString()
		if _, err := s.Begin(ctx, "transfers", key, h1); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Begin(ctx, "transfers", key, h2); !errors.Is(err, idempotency.ErrMismatch) {
			t.Fatalf("want mismatch, got %v", err)
		}
	})

	t.Run("ScopesAreIndependent", func(t *testing.T) {
		s, key := newStore(t), uuid.NewString()
		if _, err := s.Begin(ctx, "a", key, h1); err != nil {
			t.Fatal(err)
		}
		if rec, err := s.Begin(ctx, "b", key, h2); err != nil || rec != nil {
			t.Fatalf("other scope: %v, %v", rec, err)
		}
	})

	t.Run("ReleaseAllowsRetry", func(t *testing.T) {
		s, key := newStore(t), uuid.NewString()
		if _, err := s.Begin(ctx, "a", key, h1); err != nil {
			t.Fatal(err)
		}
		if err := s.Release(ctx, "a", key); err != nil {
			t.Fatal(err)
		}
		if rec, err := s.Begin(ctx, "a", key, h2); err != nil || rec != nil {
			t.Fatalf("after release: %v, %v", rec, err)
		}
	})

	t.Run("Purge", func(t *testing.T) {
		s, key := newStore(t), uuid.NewString()
		if _, err := s.Begin(ctx, "a", key, h1); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Purge(ctx, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if rec, err := s.Begin(ctx, "a", key, h2); err != nil || rec != nil {
			t.Fatalf("after purge: %v, %v", rec, err)
		}
	})
}
