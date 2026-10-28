package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MahyarNemati/ledger/internal/outbox"
)

// OutboxSource reads unpublished rows from the outbox table.
type OutboxSource struct {
	pool *pgxpool.Pool
}

func NewOutboxSource(pool *pgxpool.Pool) *OutboxSource { return &OutboxSource{pool: pool} }

var _ outbox.Source = (*OutboxSource)(nil)

// Process holds the batch's row locks while fn publishes it, so a crash
// before commit leaves the rows unpublished and they are sent again.
func (s *OutboxSource) Process(ctx context.Context, limit int, fn func(context.Context, []outbox.Message) error) (int, error) {
	var count int
	var publishErr error
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Only one relay per schema is active at a time, which is what keeps
		// per-key ordering intact when several replicas run.
		var leader bool
		if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext(current_schema() || ':outbox-relay'))`).Scan(&leader); err != nil {
			return err
		}
		if !leader {
			return nil
		}
		rows, err := tx.Query(ctx, `
			SELECT seq, id, topic, msg_key, payload, headers FROM outbox
			WHERE published_at IS NULL ORDER BY seq LIMIT $1 FOR UPDATE`, limit)
		if err != nil {
			return err
		}
		msgs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (outbox.Message, error) {
			var m outbox.Message
			var headers []byte
			if err := r.Scan(&m.Seq, &m.ID, &m.Topic, &m.Key, &m.Payload, &headers); err != nil {
				return m, err
			}
			return m, json.Unmarshal(headers, &m.Headers)
		})
		if err != nil || len(msgs) == 0 {
			return err
		}
		seqs := make([]int64, len(msgs))
		for i, m := range msgs {
			seqs[i] = m.Seq
		}
		if publishErr = fn(ctx, msgs); publishErr != nil {
			_, err := tx.Exec(ctx, `UPDATE outbox SET attempts = attempts + 1, last_error = $2 WHERE seq = ANY($1)`,
				seqs, truncate(publishErr.Error(), 1000))
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now(), attempts = attempts + 1, last_error = NULL WHERE seq = ANY($1)`, seqs); err != nil {
			return err
		}
		count = len(msgs)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("outbox: %w", err)
	}
	if publishErr != nil {
		return 0, fmt.Errorf("outbox publish: %w", publishErr)
	}
	return count, nil
}

// Backlog returns the number of unpublished messages.
func (s *OutboxSource) Backlog(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
