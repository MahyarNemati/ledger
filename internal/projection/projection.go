// Package projection builds read models (balances and statements) from the
// global event log. The apply logic is backend-agnostic; storage is plugged in
// through Writer.
package projection

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/MahyarNemati/ledger/internal/domain"
	"github.com/MahyarNemati/ledger/internal/eventstore"
)

// ErrNotFound is returned by readers for unknown accounts.
var ErrNotFound = errors.New("projection: not found")

// Balance is the projected balance of one account.
type Balance struct {
	AccountID      uuid.UUID          `json:"account_id"`
	Type           domain.AccountType `json:"type"`
	Currency       string             `json:"currency"`
	Debits         int64              `json:"debits"`
	Credits        int64              `json:"credits"`
	PendingDebits  int64              `json:"pending_debits"`
	PendingCredits int64              `json:"pending_credits"`
	Position       int64              `json:"position"`
}

// Posted returns the balance on the account's normal side.
func (b Balance) Posted() int64 {
	if b.Type.NormalSide() == domain.Debit {
		return b.Debits - b.Credits
	}
	return b.Credits - b.Debits
}

// StatementLine is one posting on an account statement.
type StatementLine struct {
	AccountID    uuid.UUID   `json:"account_id"`
	Position     int64       `json:"position"`
	EntryID      uuid.UUID   `json:"entry_id"`
	Side         domain.Side `json:"side"`
	Amount       int64       `json:"amount"`
	BalanceAfter int64       `json:"balance_after"`
	Description  string      `json:"description"`
	PostedAt     time.Time   `json:"posted_at"`
}

// Writer is the storage port used while applying events.
type Writer interface {
	GetBalance(ctx context.Context, accountID uuid.UUID) (Balance, error)
	PutBalance(ctx context.Context, b Balance) error
	AddStatementLine(ctx context.Context, l StatementLine) error
}

// Reader serves projected data to the API.
type Reader interface {
	Balance(ctx context.Context, accountID uuid.UUID) (Balance, error)
	Statement(ctx context.Context, accountID uuid.UUID, afterPosition int64, limit int) ([]StatementLine, error)
	Checkpoint(ctx context.Context, name string) (int64, error)
}

// Apply folds one stored event into the read model. Events that do not affect
// balances are ignored.
func Apply(ctx context.Context, w Writer, stored eventstore.Event) error {
	switch stored.Type {
	case "AccountOpened", "AccountPosted", "HoldReserved", "HoldReleased":
	default:
		return nil
	}
	ev, err := domain.Decode(stored.Type, stored.Data)
	if err != nil {
		return err
	}
	switch e := ev.(type) {
	case domain.AccountOpened:
		return w.PutBalance(ctx, Balance{AccountID: e.AccountID, Type: e.Type, Currency: e.Currency, Position: stored.GlobalPosition})
	case domain.AccountPosted:
		b, err := w.GetBalance(ctx, e.AccountID)
		if err != nil {
			return fmt.Errorf("posting for %s: %w", e.AccountID, err)
		}
		if e.Side == domain.Debit {
			b.Debits += e.Amount
		} else {
			b.Credits += e.Amount
		}
		b.Position = stored.GlobalPosition
		if err := w.PutBalance(ctx, b); err != nil {
			return err
		}
		return w.AddStatementLine(ctx, StatementLine{
			AccountID: e.AccountID, Position: stored.GlobalPosition, EntryID: e.EntryID, Side: e.Side,
			Amount: e.Amount, BalanceAfter: b.Posted(), Description: e.Description, PostedAt: e.PostedAt,
		})
	case domain.HoldReserved:
		return adjustPending(ctx, w, e.AccountID, e.Side, e.Amount, stored.GlobalPosition)
	case domain.HoldReleased:
		return adjustPending(ctx, w, e.AccountID, e.Side, -e.Amount, stored.GlobalPosition)
	}
	return nil
}

func adjustPending(ctx context.Context, w Writer, id uuid.UUID, side domain.Side, delta, pos int64) error {
	b, err := w.GetBalance(ctx, id)
	if err != nil {
		return fmt.Errorf("pending for %s: %w", id, err)
	}
	if side == domain.Debit {
		b.PendingDebits += delta
	} else {
		b.PendingCredits += delta
	}
	b.Position = pos
	return w.PutBalance(ctx, b)
}
