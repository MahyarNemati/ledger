package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/MahyarNemati/ledger/internal/money"
)

// EntryKind records why an entry was created.
type EntryKind string

const (
	KindJournal  EntryKind = "journal"
	KindTransfer EntryKind = "transfer"
	KindCapture  EntryKind = "capture"
	KindReversal EntryKind = "reversal"
)

// MaxLines bounds the size of a single journal entry.
const MaxLines = 64

// Line is one debit or credit of a journal entry.
type Line struct {
	AccountID uuid.UUID `json:"account_id"`
	Side      Side      `json:"side"`
	Amount    int64     `json:"amount"`
}

// Entry is an immutable, balanced journal entry. The only state change after
// posting is being marked reversed by a later, opposite entry.
type Entry struct {
	ID          uuid.UUID  `json:"id"`
	Kind        EntryKind  `json:"kind"`
	Currency    string     `json:"currency"`
	Description string     `json:"description"`
	Lines       []Line     `json:"lines"`
	Reverses    *uuid.UUID `json:"reverses,omitempty"`
	ReversedBy  *uuid.UUID `json:"reversed_by,omitempty"`
	HoldID      *uuid.UUID `json:"hold_id,omitempty"`
	PostedAt    time.Time  `json:"posted_at"`

	version int64
	changes []Event
}

// ValidateLines enforces the double-entry rules on a set of lines.
func ValidateLines(lines []Line) error {
	if len(lines) < 2 {
		return Errorf(CodeValidation, "an entry needs at least two lines")
	}
	if len(lines) > MaxLines {
		return Errorf(CodeValidation, "an entry may have at most %d lines", MaxLines)
	}
	var debits, credits int64
	seen := make(map[uuid.UUID]struct{}, len(lines))
	for i, l := range lines {
		if l.AccountID == uuid.Nil {
			return Errorf(CodeValidation, "line %d: account_id is required", i)
		}
		if _, dup := seen[l.AccountID]; dup {
			return Errorf(CodeValidation, "line %d: account %s appears more than once", i, l.AccountID)
		}
		seen[l.AccountID] = struct{}{}
		if !l.Side.Valid() {
			return Errorf(CodeValidation, "line %d: side must be debit or credit", i)
		}
		if l.Amount <= 0 {
			return Errorf(CodeValidation, "line %d: amount must be positive", i)
		}
		var err error
		if l.Side == Debit {
			debits, err = money.Add(debits, l.Amount)
		} else {
			credits, err = money.Add(credits, l.Amount)
		}
		if err != nil {
			return Errorf(CodeValidation, "line amounts overflow")
		}
	}
	if debits != credits {
		return Errorf(CodeUnbalanced, "debits %d do not equal credits %d", debits, credits)
	}
	return nil
}

// EntryParams describes a new journal entry.
type EntryParams struct {
	ID          uuid.UUID
	Kind        EntryKind
	Currency    string
	Description string
	Lines       []Line
	Reverses    *uuid.UUID
	HoldID      *uuid.UUID
}

// NewEntry validates and creates a posted entry. Account-level checks
// (currency, status, funds) are the caller's responsibility because they span
// other aggregates.
func NewEntry(p EntryParams, now time.Time) (*Entry, error) {
	cur, err := money.LookupCurrency(p.Currency)
	if err != nil {
		return nil, Errorf(CodeValidation, "%v", err)
	}
	if err := ValidateLines(p.Lines); err != nil {
		return nil, err
	}
	desc := strings.TrimSpace(p.Description)
	if len(desc) > 500 {
		return nil, Errorf(CodeValidation, "description must be at most 500 characters")
	}
	kind := p.Kind
	if kind == "" {
		kind = KindJournal
	}
	e := &Entry{}
	e.raise(EntryPosted{
		EntryID: p.ID, Kind: kind, Currency: cur.Code, Description: desc,
		Lines: append([]Line(nil), p.Lines...), Reverses: p.Reverses, HoldID: p.HoldID, PostedAt: now,
	})
	return e, nil
}

// Version is the stream version the aggregate was loaded at.
func (e *Entry) Version() int64 { return e.version }

// Changes are events raised since load.
func (e *Entry) Changes() []Event { return e.changes }

// Exists reports whether the entry has been posted.
func (e *Entry) Exists() bool { return e.ID != uuid.Nil }

// ReversalLines mirrors every line onto the opposite side.
func (e *Entry) ReversalLines() []Line {
	out := make([]Line, len(e.Lines))
	for i, l := range e.Lines {
		out[i] = Line{AccountID: l.AccountID, Side: l.Side.Opposite(), Amount: l.Amount}
	}
	return out
}

// MarkReversed records that reversalID undoes this entry.
func (e *Entry) MarkReversed(reversalID uuid.UUID, reason string, now time.Time) error {
	if !e.Exists() {
		return Errorf(CodeNotFound, "entry not found")
	}
	if e.Kind == KindReversal {
		return Errorf(CodeNotReversible, "reversal entries cannot be reversed; post a new entry instead")
	}
	if e.ReversedBy != nil {
		return Errorf(CodeAlreadyReversed, "entry %s was already reversed by %s", e.ID, *e.ReversedBy)
	}
	e.raise(EntryReversed{EntryID: e.ID, ReversalEntryID: reversalID, Reason: strings.TrimSpace(reason), ReversedAt: now})
	return nil
}

func (e *Entry) raise(ev Event) {
	if err := e.Apply(ev); err != nil {
		panic(fmt.Sprintf("entry: applying freshly raised event: %v", err))
	}
	e.changes = append(e.changes, ev)
}

// Apply mutates state from an event.
func (e *Entry) Apply(ev Event) error {
	switch v := ev.(type) {
	case EntryPosted:
		e.ID, e.Kind, e.Currency, e.Description = v.EntryID, v.Kind, v.Currency, v.Description
		e.Lines, e.Reverses, e.HoldID, e.PostedAt = v.Lines, v.Reverses, v.HoldID, v.PostedAt
	case EntryReversed:
		id := v.ReversalEntryID
		e.ReversedBy = &id
	default:
		return fmt.Errorf("entry: unexpected event %T", ev)
	}
	return nil
}

// Loaded marks the aggregate as rebuilt at version v.
func (e *Entry) Loaded(v int64) { e.version, e.changes = v, nil }
