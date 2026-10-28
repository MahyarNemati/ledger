package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/MahyarNemati/ledger/internal/money"
)

// HoldStatus is the lifecycle state of a hold.
type HoldStatus string

const (
	HoldStatusPending  HoldStatus = "pending"
	HoldStatusCaptured HoldStatus = "captured"
	HoldStatusVoided   HoldStatus = "voided"
)

// MaxHoldTTL caps how long an authorization may stay open.
const MaxHoldTTL = 30 * 24 * time.Hour

// Hold is an authorization: a pending transfer that reserves funds on both
// accounts until it is captured (fully or partially) or voided.
type Hold struct {
	ID              uuid.UUID  `json:"id"`
	DebitAccountID  uuid.UUID  `json:"debit_account_id"`
	CreditAccountID uuid.UUID  `json:"credit_account_id"`
	Currency        string     `json:"currency"`
	Amount          int64      `json:"amount"`
	Description     string     `json:"description"`
	Status          HoldStatus `json:"status"`
	CapturedAmount  int64      `json:"captured_amount"`
	EntryID         *uuid.UUID `json:"entry_id,omitempty"`
	ExpiresAt       time.Time  `json:"expires_at"`
	PlacedAt        time.Time  `json:"placed_at"`

	version int64
	changes []Event
}

// PlaceHold validates and creates a pending hold.
func PlaceHold(id, debitID, creditID uuid.UUID, currency string, amount int64, description string, expiresAt, now time.Time) (*Hold, error) {
	cur, err := money.LookupCurrency(currency)
	if err != nil {
		return nil, Errorf(CodeValidation, "%v", err)
	}
	if debitID == uuid.Nil || creditID == uuid.Nil || debitID == creditID {
		return nil, Errorf(CodeValidation, "a hold needs two distinct accounts")
	}
	if amount <= 0 {
		return nil, Errorf(CodeValidation, "amount must be positive")
	}
	if !expiresAt.After(now) || expiresAt.Sub(now) > MaxHoldTTL {
		return nil, Errorf(CodeValidation, "expires_at must be in the future and within %s", MaxHoldTTL)
	}
	h := &Hold{}
	h.raise(HoldPlaced{
		HoldID: id, DebitAccountID: debitID, CreditAccountID: creditID, Currency: cur.Code,
		Amount: amount, Description: strings.TrimSpace(description), ExpiresAt: expiresAt, PlacedAt: now,
	})
	return h, nil
}

// Version is the stream version the aggregate was loaded at.
func (h *Hold) Version() int64 { return h.version }

// Changes are events raised since load.
func (h *Hold) Changes() []Event { return h.changes }

// Exists reports whether the hold has been placed.
func (h *Hold) Exists() bool { return h.ID != uuid.Nil }

// Expired reports whether a pending hold is past its expiry.
func (h *Hold) Expired(now time.Time) bool {
	return h.Status == HoldStatusPending && !now.Before(h.ExpiresAt)
}

// Capture settles up to the held amount; any remainder is released.
func (h *Hold) Capture(entryID uuid.UUID, amount int64, now time.Time) error {
	if !h.Exists() {
		return Errorf(CodeNotFound, "hold not found")
	}
	if h.Status != HoldStatusPending {
		return Errorf(CodeHoldNotPending, "hold %s is %s", h.ID, h.Status)
	}
	if h.Expired(now) {
		return Errorf(CodeHoldExpired, "hold %s expired at %s", h.ID, h.ExpiresAt.Format(time.RFC3339))
	}
	if amount <= 0 || amount > h.Amount {
		return Errorf(CodeValidation, "capture amount must be between 1 and %d", h.Amount)
	}
	h.raise(HoldCaptured{HoldID: h.ID, EntryID: entryID, Amount: amount, CapturedAt: now})
	return nil
}

// Void cancels a pending hold. Expired holds can still be voided, which is
// how their reservations get cleaned up.
func (h *Hold) Void(reason string, now time.Time) error {
	if !h.Exists() {
		return Errorf(CodeNotFound, "hold not found")
	}
	if h.Status != HoldStatusPending {
		return Errorf(CodeHoldNotPending, "hold %s is %s", h.ID, h.Status)
	}
	h.raise(HoldVoided{HoldID: h.ID, Reason: strings.TrimSpace(reason), VoidedAt: now})
	return nil
}

func (h *Hold) raise(ev Event) {
	if err := h.Apply(ev); err != nil {
		panic(fmt.Sprintf("hold: applying freshly raised event: %v", err))
	}
	h.changes = append(h.changes, ev)
}

// Apply mutates state from an event.
func (h *Hold) Apply(ev Event) error {
	switch v := ev.(type) {
	case HoldPlaced:
		h.ID, h.DebitAccountID, h.CreditAccountID = v.HoldID, v.DebitAccountID, v.CreditAccountID
		h.Currency, h.Amount, h.Description = v.Currency, v.Amount, v.Description
		h.ExpiresAt, h.PlacedAt, h.Status = v.ExpiresAt, v.PlacedAt, HoldStatusPending
	case HoldCaptured:
		id := v.EntryID
		h.Status, h.CapturedAmount, h.EntryID = HoldStatusCaptured, v.Amount, &id
	case HoldVoided:
		h.Status = HoldStatusVoided
	default:
		return fmt.Errorf("hold: unexpected event %T", ev)
	}
	return nil
}

// Loaded marks the aggregate as rebuilt at version v.
func (h *Hold) Loaded(v int64) { h.version, h.changes = v, nil }
