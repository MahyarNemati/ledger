package projection

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/MahyarNemati/ledger/internal/eventstore"
)

// Tx is a transactional view of the read-model store.
type Tx interface {
	Writer
	// Checkpoint returns the last applied global position and locks it for
	// the duration of the transaction.
	Checkpoint(ctx context.Context, name string) (int64, error)
	SetCheckpoint(ctx context.Context, name string, position int64) error
}

// Backend runs fn inside a transaction that commits only if fn succeeds.
type Backend interface {
	InTx(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
}

// Runner tails the event log and keeps a projection up to date.
type Runner struct {
	Name     string
	Store    eventstore.Store
	Backend  Backend
	Batch    int
	Interval time.Duration
	Log      *slog.Logger
}

// Step applies at most one batch and returns how many events it applied.
func (r *Runner) Step(ctx context.Context) (int, error) {
	batch := r.Batch
	if batch <= 0 {
		batch = 500
	}
	applied := 0
	err := r.Backend.InTx(ctx, func(ctx context.Context, tx Tx) error {
		cp, err := tx.Checkpoint(ctx, r.Name)
		if err != nil {
			return err
		}
		events, err := r.Store.ReadAll(ctx, cp, batch)
		if err != nil {
			return err
		}
		for _, ev := range events {
			if err := Apply(ctx, tx, ev); err != nil {
				return fmt.Errorf("apply position %d (%s): %w", ev.GlobalPosition, ev.Type, err)
			}
		}
		if len(events) == 0 {
			return nil
		}
		applied = len(events)
		return tx.SetCheckpoint(ctx, r.Name, events[len(events)-1].GlobalPosition)
	})
	return applied, err
}

// CatchUp applies batches until the projection reaches the head of the log.
func (r *Runner) CatchUp(ctx context.Context) error {
	for {
		n, err := r.Step(ctx)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

// Run catches up continuously until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	interval := r.Interval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	log := r.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if err := r.CatchUp(ctx); err != nil && ctx.Err() == nil {
			log.ErrorContext(ctx, "projection step failed", "projection", r.Name, "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
