package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MahyarNemati/ledger/internal/consumer"
)

// Once runs fn in a transaction that also records (consumer, messageID) in
// the inbox. If the message was already recorded fn is skipped and applied is
// false. The side effects and the dedupe record commit or roll back together.
func Once(ctx context.Context, pool *pgxpool.Pool, consumerName string, messageID uuid.UUID, fn func(pgx.Tx) error) (applied bool, err error) {
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO inbox (consumer, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, consumerName, messageID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		applied = true
		return fn(tx)
	})
	if err != nil {
		return false, fmt.Errorf("inbox %s/%s: %w", consumerName, messageID, err)
	}
	return applied, nil
}

// ActivityStore is the Postgres backing for the example consumer.
type ActivityStore struct {
	pool *pgxpool.Pool
}

func NewActivityStore(pool *pgxpool.Pool) *ActivityStore { return &ActivityStore{pool: pool} }

var _ consumer.Store = (*ActivityStore)(nil)

func (s *ActivityStore) ApplyPosting(ctx context.Context, name string, msgID, accountID, entryID uuid.UUID) (bool, error) {
	return Once(ctx, s.pool, name, msgID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO account_activity (account_id, postings, last_entry) VALUES ($1, 1, $2)
			ON CONFLICT (account_id) DO UPDATE SET
				postings = account_activity.postings + 1, last_entry = EXCLUDED.last_entry, updated_at = now()`,
			accountID, entryID)
		return err
	})
}

func (s *ActivityStore) Activity(ctx context.Context, id uuid.UUID) (consumer.Activity, error) {
	a := consumer.Activity{AccountID: id}
	var last *uuid.UUID
	err := s.pool.QueryRow(ctx, `SELECT postings, last_entry FROM account_activity WHERE account_id = $1`, id).Scan(&a.Postings, &last)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, nil
	}
	if last != nil {
		a.LastEntry = *last
	}
	return a, err
}
