package httpapi

import (
	"time"

	"github.com/google/uuid"

	"github.com/MahyarNemati/ledger/internal/domain"
	"github.com/MahyarNemati/ledger/internal/money"
	"github.com/MahyarNemati/ledger/internal/projection"
)

type openAccountRequest struct {
	Name           string             `json:"name"`
	Type           domain.AccountType `json:"type"`
	Currency       string             `json:"currency"`
	AllowOverdraft bool               `json:"allow_overdraft"`
	OverdraftLimit int64              `json:"overdraft_limit"`
}

type lineRequest struct {
	AccountID uuid.UUID   `json:"account_id"`
	Side      domain.Side `json:"side"`
	Amount    int64       `json:"amount"`
}

type postEntryRequest struct {
	Currency    string        `json:"currency"`
	Description string        `json:"description"`
	Lines       []lineRequest `json:"lines"`
}

type transferRequest struct {
	DebitAccountID  uuid.UUID `json:"debit_account_id"`
	CreditAccountID uuid.UUID `json:"credit_account_id"`
	Amount          int64     `json:"amount"`
	Currency        string    `json:"currency"`
	Description     string    `json:"description"`
}

type reverseRequest struct {
	Reason string `json:"reason"`
}

type placeHoldRequest struct {
	DebitAccountID  uuid.UUID  `json:"debit_account_id"`
	CreditAccountID uuid.UUID  `json:"credit_account_id"`
	Amount          int64      `json:"amount"`
	Currency        string     `json:"currency"`
	Description     string     `json:"description"`
	ExpiresAt       *time.Time `json:"expires_at"`
	TTLSeconds      int64      `json:"ttl_seconds"`
}

type captureRequest struct {
	Amount int64 `json:"amount"`
}

type voidRequest struct {
	Reason string `json:"reason"`
}

type accountResponse struct {
	ID             uuid.UUID            `json:"id"`
	Name           string               `json:"name"`
	Type           domain.AccountType   `json:"type"`
	Currency       string               `json:"currency"`
	Status         domain.AccountStatus `json:"status"`
	Balance        int64                `json:"balance"`
	BalanceDecimal string               `json:"balance_decimal"`
	Available      int64                `json:"available"`
	Debits         int64                `json:"debits"`
	Credits        int64                `json:"credits"`
	PendingDebits  int64                `json:"pending_debits"`
	PendingCredits int64                `json:"pending_credits"`
	AllowOverdraft bool                 `json:"allow_overdraft"`
	OverdraftLimit int64                `json:"overdraft_limit"`
	Version        int64                `json:"version"`
	OpenedAt       time.Time            `json:"opened_at"`
}

func toAccount(a *domain.Account, now time.Time) accountResponse {
	cur, _ := money.LookupCurrency(a.Currency)
	return accountResponse{
		ID: a.ID, Name: a.Name, Type: a.Type, Currency: a.Currency, Status: a.Status,
		Balance: a.Balance(), BalanceDecimal: money.Format(a.Balance(), cur), Available: a.Available(now),
		Debits: a.Debits, Credits: a.Credits,
		PendingDebits: a.Pending(domain.Debit), PendingCredits: a.Pending(domain.Credit),
		AllowOverdraft: a.Policy.AllowOverdraft, OverdraftLimit: a.Policy.OverdraftLimit,
		Version: a.Version() + int64(len(a.Changes())), OpenedAt: a.OpenedAt,
	}
}

type entryResponse struct {
	ID          uuid.UUID        `json:"id"`
	Kind        domain.EntryKind `json:"kind"`
	Currency    string           `json:"currency"`
	Description string           `json:"description"`
	Lines       []domain.Line    `json:"lines"`
	Reverses    *uuid.UUID       `json:"reverses,omitempty"`
	ReversedBy  *uuid.UUID       `json:"reversed_by,omitempty"`
	HoldID      *uuid.UUID       `json:"hold_id,omitempty"`
	PostedAt    time.Time        `json:"posted_at"`
}

func toEntry(e *domain.Entry) entryResponse {
	return entryResponse{
		ID: e.ID, Kind: e.Kind, Currency: e.Currency, Description: e.Description, Lines: e.Lines,
		Reverses: e.Reverses, ReversedBy: e.ReversedBy, HoldID: e.HoldID, PostedAt: e.PostedAt,
	}
}

type holdResponse struct {
	ID              uuid.UUID         `json:"id"`
	DebitAccountID  uuid.UUID         `json:"debit_account_id"`
	CreditAccountID uuid.UUID         `json:"credit_account_id"`
	Currency        string            `json:"currency"`
	Amount          int64             `json:"amount"`
	Description     string            `json:"description"`
	Status          domain.HoldStatus `json:"status"`
	Expired         bool              `json:"expired"`
	CapturedAmount  int64             `json:"captured_amount"`
	EntryID         *uuid.UUID        `json:"entry_id,omitempty"`
	ExpiresAt       time.Time         `json:"expires_at"`
	PlacedAt        time.Time         `json:"placed_at"`
}

func toHold(h *domain.Hold, now time.Time) holdResponse {
	return holdResponse{
		ID: h.ID, DebitAccountID: h.DebitAccountID, CreditAccountID: h.CreditAccountID, Currency: h.Currency, Amount: h.Amount,
		Description: h.Description, Status: h.Status, Expired: h.Expired(now), CapturedAmount: h.CapturedAmount,
		EntryID: h.EntryID, ExpiresAt: h.ExpiresAt, PlacedAt: h.PlacedAt,
	}
}

type captureResponse struct {
	Hold  holdResponse  `json:"hold"`
	Entry entryResponse `json:"entry"`
}

type balanceResponse struct {
	projection.Balance
	Posted int64 `json:"balance"`
}

type statementResponse struct {
	AccountID  uuid.UUID                  `json:"account_id"`
	Lines      []projection.StatementLine `json:"lines"`
	NextCursor *int64                     `json:"next_cursor,omitempty"`
}
