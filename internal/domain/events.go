// Package domain holds the ledger aggregates and the events that define them.
// Nothing in here performs I/O.
package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Event is a domain fact recorded on an aggregate stream.
type Event interface {
	EventType() string
}

type AccountOpened struct {
	AccountID uuid.UUID   `json:"account_id"`
	Name      string      `json:"name"`
	Type      AccountType `json:"type"`
	Currency  string      `json:"currency"`
	Policy    Policy      `json:"policy"`
	OpenedAt  time.Time   `json:"opened_at"`
}

type AccountClosed struct {
	AccountID uuid.UUID `json:"account_id"`
	ClosedAt  time.Time `json:"closed_at"`
}

type AccountPosted struct {
	AccountID   uuid.UUID `json:"account_id"`
	EntryID     uuid.UUID `json:"entry_id"`
	Side        Side      `json:"side"`
	Amount      int64     `json:"amount"`
	Description string    `json:"description,omitempty"`
	PostedAt    time.Time `json:"posted_at"`
}

type HoldReserved struct {
	AccountID uuid.UUID `json:"account_id"`
	HoldID    uuid.UUID `json:"hold_id"`
	Side      Side      `json:"side"`
	Amount    int64     `json:"amount"`
	ExpiresAt time.Time `json:"expires_at"`
}

type HoldReleased struct {
	AccountID uuid.UUID `json:"account_id"`
	HoldID    uuid.UUID `json:"hold_id"`
	Side      Side      `json:"side"`
	Amount    int64     `json:"amount"`
}

type EntryPosted struct {
	EntryID     uuid.UUID  `json:"entry_id"`
	Kind        EntryKind  `json:"kind"`
	Currency    string     `json:"currency"`
	Description string     `json:"description,omitempty"`
	Lines       []Line     `json:"lines"`
	Reverses    *uuid.UUID `json:"reverses,omitempty"`
	HoldID      *uuid.UUID `json:"hold_id,omitempty"`
	PostedAt    time.Time  `json:"posted_at"`
}

type EntryReversed struct {
	EntryID         uuid.UUID `json:"entry_id"`
	ReversalEntryID uuid.UUID `json:"reversal_entry_id"`
	Reason          string    `json:"reason,omitempty"`
	ReversedAt      time.Time `json:"reversed_at"`
}

type HoldPlaced struct {
	HoldID          uuid.UUID `json:"hold_id"`
	DebitAccountID  uuid.UUID `json:"debit_account_id"`
	CreditAccountID uuid.UUID `json:"credit_account_id"`
	Currency        string    `json:"currency"`
	Amount          int64     `json:"amount"`
	Description     string    `json:"description,omitempty"`
	ExpiresAt       time.Time `json:"expires_at"`
	PlacedAt        time.Time `json:"placed_at"`
}

type HoldCaptured struct {
	HoldID     uuid.UUID `json:"hold_id"`
	EntryID    uuid.UUID `json:"entry_id"`
	Amount     int64     `json:"amount"`
	CapturedAt time.Time `json:"captured_at"`
}

type HoldVoided struct {
	HoldID   uuid.UUID `json:"hold_id"`
	Reason   string    `json:"reason,omitempty"`
	VoidedAt time.Time `json:"voided_at"`
}

func (AccountOpened) EventType() string { return "AccountOpened" }
func (AccountClosed) EventType() string { return "AccountClosed" }
func (AccountPosted) EventType() string { return "AccountPosted" }
func (HoldReserved) EventType() string  { return "HoldReserved" }
func (HoldReleased) EventType() string  { return "HoldReleased" }
func (EntryPosted) EventType() string   { return "EntryPosted" }
func (EntryReversed) EventType() string { return "EntryReversed" }
func (HoldPlaced) EventType() string    { return "HoldPlaced" }
func (HoldCaptured) EventType() string  { return "HoldCaptured" }
func (HoldVoided) EventType() string    { return "HoldVoided" }

var factories = map[string]func() Event{
	"AccountOpened": func() Event { return &AccountOpened{} },
	"AccountClosed": func() Event { return &AccountClosed{} },
	"AccountPosted": func() Event { return &AccountPosted{} },
	"HoldReserved":  func() Event { return &HoldReserved{} },
	"HoldReleased":  func() Event { return &HoldReleased{} },
	"EntryPosted":   func() Event { return &EntryPosted{} },
	"EntryReversed": func() Event { return &EntryReversed{} },
	"HoldPlaced":    func() Event { return &HoldPlaced{} },
	"HoldCaptured":  func() Event { return &HoldCaptured{} },
	"HoldVoided":    func() Event { return &HoldVoided{} },
}

// Encode serialises a domain event.
func Encode(e Event) (string, json.RawMessage, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return "", nil, fmt.Errorf("encode %s: %w", e.EventType(), err)
	}
	return e.EventType(), b, nil
}

// Decode rebuilds a domain event from its stored type and payload. The
// returned value is always a non-pointer struct.
func Decode(eventType string, data []byte) (Event, error) {
	f, ok := factories[eventType]
	if !ok {
		return nil, fmt.Errorf("unknown event type %q", eventType)
	}
	ptr := f()
	if err := json.Unmarshal(data, ptr); err != nil {
		return nil, fmt.Errorf("decode %s: %w", eventType, err)
	}
	return deref(ptr), nil
}

func deref(e Event) Event {
	switch v := e.(type) {
	case *AccountOpened:
		return *v
	case *AccountClosed:
		return *v
	case *AccountPosted:
		return *v
	case *HoldReserved:
		return *v
	case *HoldReleased:
		return *v
	case *EntryPosted:
		return *v
	case *EntryReversed:
		return *v
	case *HoldPlaced:
		return *v
	case *HoldCaptured:
		return *v
	case *HoldVoided:
		return *v
	}
	return e
}

// Stream identifiers.
func AccountStream(id uuid.UUID) string { return "account-" + id.String() }
func EntryStream(id uuid.UUID) string   { return "entry-" + id.String() }
func HoldStream(id uuid.UUID) string    { return "hold-" + id.String() }
