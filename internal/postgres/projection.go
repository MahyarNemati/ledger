package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MahyarNemati/ledger/internal/domain"
	"github.com/MahyarNemati/ledger/internal/projection"
)

// Projections stores balance and statement read models.
type Projections struct {
	pool *pgxpool.Pool
}

func NewProjections(pool *pgxpool.Pool) *Projections { return &Projections{pool: pool} }

var (
	_ projection.Backend = (*Projections)(nil)
	_ projection.Reader  = (*Projections)(nil)
)

type projTx struct{ tx pgx.Tx }

func (p *Projections) InTx(ctx context.Context, fn func(context.Context, projection.Tx) error) error {
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error { return fn(ctx, projTx{tx}) })
}

// Checkpoint serialises runners with an advisory lock rather than a row lock.
// A row lock would assign this transaction an xid before it reads the event
// log, and the log's visibility guard would then hide anything committed by
// later transactions until this one finished.
func (t projTx) Checkpoint(ctx context.Context, name string) (int64, error) {
	if _, err := t.tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('projection:' || $1))`, name); err != nil {
		return 0, err
	}
	var pos int64
	err := t.tx.QueryRow(ctx, `SELECT position FROM projection_checkpoints WHERE name = $1`, name).Scan(&pos)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return pos, err
}

func (t projTx) SetCheckpoint(ctx context.Context, name string, pos int64) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO projection_checkpoints (name, position) VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET position = EXCLUDED.position`, name, pos)
	return err
}

func (t projTx) GetBalance(ctx context.Context, id uuid.UUID) (projection.Balance, error) {
	return scanBalance(t.tx.QueryRow(ctx, balanceSQL+` WHERE account_id = $1`, id))
}

func (t projTx) PutBalance(ctx context.Context, b projection.Balance) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO balances (account_id, account_type, currency, debits, credits, pending_debits, pending_credits, position)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (account_id) DO UPDATE SET
			debits = EXCLUDED.debits, credits = EXCLUDED.credits,
			pending_debits = EXCLUDED.pending_debits, pending_credits = EXCLUDED.pending_credits,
			position = EXCLUDED.position`,
		b.AccountID, string(b.Type), b.Currency, b.Debits, b.Credits, b.PendingDebits, b.PendingCredits, b.Position)
	return err
}

func (t projTx) AddStatementLine(ctx context.Context, l projection.StatementLine) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO statement_lines (account_id, position, entry_id, side, amount, balance_after, description, posted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		l.AccountID, l.Position, l.EntryID, string(l.Side), l.Amount, l.BalanceAfter, l.Description, l.PostedAt)
	return err
}

const balanceSQL = `SELECT account_id, account_type, currency, debits, credits, pending_debits, pending_credits, position FROM balances`

func scanBalance(row pgx.Row) (projection.Balance, error) {
	var b projection.Balance
	var typ string
	err := row.Scan(&b.AccountID, &typ, &b.Currency, &b.Debits, &b.Credits, &b.PendingDebits, &b.PendingCredits, &b.Position)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, projection.ErrNotFound
	}
	if err != nil {
		return b, fmt.Errorf("scan balance: %w", err)
	}
	b.Type = domain.AccountType(typ)
	return b, nil
}

func (p *Projections) Balance(ctx context.Context, id uuid.UUID) (projection.Balance, error) {
	return scanBalance(p.pool.QueryRow(ctx, balanceSQL+` WHERE account_id = $1`, id))
}

func (p *Projections) Statement(ctx context.Context, id uuid.UUID, after int64, limit int) ([]projection.StatementLine, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT account_id, position, entry_id, side, amount, balance_after, description, posted_at
		FROM statement_lines WHERE account_id = $1 AND position > $2 ORDER BY position LIMIT $3`, id, after, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (projection.StatementLine, error) {
		var l projection.StatementLine
		var side string
		err := r.Scan(&l.AccountID, &l.Position, &l.EntryID, &side, &l.Amount, &l.BalanceAfter, &l.Description, &l.PostedAt)
		l.Side = domain.Side(side)
		return l, err
	})
}

func (p *Projections) Checkpoint(ctx context.Context, name string) (int64, error) {
	var pos int64
	err := p.pool.QueryRow(ctx, `SELECT position FROM projection_checkpoints WHERE name = $1`, name).Scan(&pos)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return pos, err
}
