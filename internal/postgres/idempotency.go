package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MahyarNemati/ledger/internal/idempotency"
)

// IdempotencyStore keeps request outcomes in Postgres.
type IdempotencyStore struct {
	pool *pgxpool.Pool
	// StaleAfter is how long an unfinished claim blocks retries before it is
	// assumed abandoned by a crashed process.
	StaleAfter time.Duration
}

func NewIdempotencyStore(pool *pgxpool.Pool) *IdempotencyStore {
	return &IdempotencyStore{pool: pool, StaleAfter: time.Minute}
}

var _ idempotency.Store = (*IdempotencyStore)(nil)

func (s *IdempotencyStore) Begin(ctx context.Context, scope, key string, hash []byte) (*idempotency.Record, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO idempotency_keys (scope, key, request_hash) VALUES ($1, $2, $3)
		ON CONFLICT (scope, key) DO NOTHING`, scope, key, hash)
	if err != nil {
		return nil, fmt.Errorf("idempotency begin: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil, nil
	}

	var stored []byte
	var status *int
	var body []byte
	var completed *time.Time
	var created time.Time
	err = s.pool.QueryRow(ctx, `
		SELECT request_hash, status_code, response, completed_at, created_at
		FROM idempotency_keys WHERE scope = $1 AND key = $2`, scope, key).Scan(&stored, &status, &body, &completed, &created)
	if errors.Is(err, pgx.ErrNoRows) {
		// Released between our insert and select; let the client retry.
		return nil, idempotency.ErrInProgress
	}
	if err != nil {
		return nil, fmt.Errorf("idempotency lookup: %w", err)
	}
	if !bytes.Equal(stored, hash) {
		return nil, idempotency.ErrMismatch
	}
	if completed != nil && status != nil {
		return &idempotency.Record{StatusCode: *status, Body: body}, nil
	}
	tag, err = s.pool.Exec(ctx, `
		UPDATE idempotency_keys SET created_at = now()
		WHERE scope = $1 AND key = $2 AND completed_at IS NULL AND created_at < now() - $3::interval`,
		scope, key, s.StaleAfter.String())
	if err != nil {
		return nil, fmt.Errorf("idempotency takeover: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil, nil
	}
	return nil, idempotency.ErrInProgress
}

func (s *IdempotencyStore) Complete(ctx context.Context, scope, key string, rec idempotency.Record) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE idempotency_keys SET status_code = $3, response = $4, completed_at = now()
		WHERE scope = $1 AND key = $2`, scope, key, rec.StatusCode, rec.Body)
	if err != nil {
		return fmt.Errorf("idempotency complete: %w", err)
	}
	return nil
}

func (s *IdempotencyStore) Release(ctx context.Context, scope, key string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE scope = $1 AND key = $2 AND completed_at IS NULL`, scope, key)
	return err
}

func (s *IdempotencyStore) Purge(ctx context.Context, before time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE created_at < $1`, before)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
