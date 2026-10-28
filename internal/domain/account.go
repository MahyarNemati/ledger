package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/MahyarNemati/ledger/internal/money"
)

// Side is the debit or credit side of a posting.
type Side string

const (
	Debit  Side = "debit"
	Credit Side = "credit"
)

func (s Side) Valid() bool { return s == Debit || s == Credit }

// Opposite returns the other side.
func (s Side) Opposite() Side {
	if s == Debit {
		return Credit
	}
	return Debit
}

// AccountType is the accounting classification of an account.
type AccountType string

const (
	Asset     AccountType = "asset"
	Liability AccountType = "liability"
	Equity    AccountType = "equity"
	Revenue   AccountType = "revenue"
	Expense   AccountType = "expense"
)

func (t AccountType) Valid() bool {
	switch t {
	case Asset, Liability, Equity, Revenue, Expense:
		return true
	}
	return false
}

// NormalSide is the side that increases the account's balance.
func (t AccountType) NormalSide() Side {
	if t == Asset || t == Expense {
		return Debit
	}
	return Credit
}

// Policy controls whether an account may go below zero on its normal side.
type Policy struct {
	AllowOverdraft bool  `json:"allow_overdraft"`
	OverdraftLimit int64 `json:"overdraft_limit"`
}

// AccountStatus is the lifecycle state of an account.
type AccountStatus string

const (
	StatusOpen   AccountStatus = "open"
	StatusClosed AccountStatus = "closed"
)

// Reservation is the account-side view of a pending hold.
type Reservation struct {
	Side      Side      `json:"side"`
	Amount    int64     `json:"amount"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Account is the aggregate that owns balances and overdraft enforcement.
// Debits and Credits are lifetime totals; the signed balance is derived.
type Account struct {
	ID           uuid.UUID                 `json:"id"`
	Name         string                    `json:"name"`
	Type         AccountType               `json:"type"`
	Currency     string                    `json:"currency"`
	Policy       Policy                    `json:"policy"`
	Status       AccountStatus             `json:"status"`
	Debits       int64                     `json:"debits"`
	Credits      int64                     `json:"credits"`
	Reservations map[uuid.UUID]Reservation `json:"reservations"`
	OpenedAt     time.Time                 `json:"opened_at"`

	version int64
	changes []Event
}

// OpenAccount validates input and produces a new account aggregate.
func OpenAccount(id uuid.UUID, name string, typ AccountType, currency string, p Policy, now time.Time) (*Account, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 200 {
		return nil, Errorf(CodeValidation, "name must be 1-200 characters")
	}
	if !typ.Valid() {
		return nil, Errorf(CodeValidation, "unknown account type %q", typ)
	}
	cur, err := money.LookupCurrency(currency)
	if err != nil {
		return nil, Errorf(CodeValidation, "%v", err)
	}
	if p.OverdraftLimit < 0 {
		return nil, Errorf(CodeValidation, "overdraft_limit must not be negative")
	}
	a := &Account{}
	a.raise(AccountOpened{AccountID: id, Name: name, Type: typ, Currency: cur.Code, Policy: p, OpenedAt: now})
	return a, nil
}

// Version is the stream version the aggregate was loaded at.
func (a *Account) Version() int64 { return a.version }

// Changes are events raised since load.
func (a *Account) Changes() []Event { return a.changes }

// Exists reports whether the account has been opened.
func (a *Account) Exists() bool { return a.ID != uuid.Nil }

// SignedBalance is debits minus credits; the sum across all accounts of a
// currency is always zero.
func (a *Account) SignedBalance() int64 { return a.Debits - a.Credits }

// Balance is the posted balance measured on the account's normal side.
func (a *Account) Balance() int64 {
	if a.Type.NormalSide() == Debit {
		return a.Debits - a.Credits
	}
	return a.Credits - a.Debits
}

// Pending returns the sum of unreleased reservations on a side, including
// expired ones that have not been voided yet.
func (a *Account) Pending(side Side) int64 {
	var sum int64
	for _, r := range a.Reservations {
		if r.Side == side {
			sum += r.Amount
		}
	}
	return sum
}

// Available is the balance minus live reservations that would reduce it.
func (a *Account) Available(now time.Time) int64 {
	reducing := a.Type.NormalSide().Opposite()
	avail := a.Balance()
	for _, r := range a.Reservations {
		if r.Side == reducing && now.Before(r.ExpiresAt) {
			avail -= r.Amount
		}
	}
	return avail
}

func (a *Account) ensureOpen() error {
	if !a.Exists() {
		return Errorf(CodeNotFound, "account not found")
	}
	if a.Status != StatusOpen {
		return Errorf(CodeAccountClosed, "account %s is closed", a.ID)
	}
	return nil
}

// checkFunds verifies that moving amount on side, after releasing a prior
// reservation of `released` on the same side, keeps the account within policy.
func (a *Account) checkFunds(side Side, amount, released int64, now time.Time) error {
	if side == a.Type.NormalSide() || a.Policy.AllowOverdraft {
		return nil
	}
	after := a.Available(now) + released - amount
	if after < -a.Policy.OverdraftLimit {
		return Errorf(CodeInsufficientFunds, "account %s has %d available, needs %d", a.ID, a.Available(now)+released, amount)
	}
	return nil
}

// Post applies one journal line to the account.
func (a *Account) Post(entryID uuid.UUID, side Side, amount int64, description string, now time.Time) error {
	if err := a.ensureOpen(); err != nil {
		return err
	}
	if err := a.checkFunds(side, amount, 0, now); err != nil {
		return err
	}
	if err := a.checkTotals(side, amount); err != nil {
		return err
	}
	a.raise(AccountPosted{AccountID: a.ID, EntryID: entryID, Side: side, Amount: amount, Description: description, PostedAt: now})
	return nil
}

// Reserve places a pending reservation for a hold.
func (a *Account) Reserve(holdID uuid.UUID, side Side, amount int64, expiresAt, now time.Time) error {
	if err := a.ensureOpen(); err != nil {
		return err
	}
	if _, dup := a.Reservations[holdID]; dup {
		return Errorf(CodeAlreadyExists, "hold %s already reserved on account %s", holdID, a.ID)
	}
	if err := a.checkFunds(side, amount, 0, now); err != nil {
		return err
	}
	a.raise(HoldReserved{AccountID: a.ID, HoldID: holdID, Side: side, Amount: amount, ExpiresAt: expiresAt})
	return nil
}

// Settle releases a reservation and posts the captured amount in one step,
// so the funds check accounts for the reservation being given back.
func (a *Account) Settle(holdID, entryID uuid.UUID, amount int64, description string, now time.Time) error {
	r, ok := a.Reservations[holdID]
	if !ok {
		return Errorf(CodeHoldNotPending, "no reservation for hold %s on account %s", holdID, a.ID)
	}
	if err := a.ensureOpen(); err != nil {
		return err
	}
	released := r.Amount
	if !now.Before(r.ExpiresAt) {
		released = 0
	}
	if err := a.checkFunds(r.Side, amount, released, now); err != nil {
		return err
	}
	if err := a.checkTotals(r.Side, amount); err != nil {
		return err
	}
	a.raise(HoldReleased{AccountID: a.ID, HoldID: holdID, Side: r.Side, Amount: r.Amount})
	a.raise(AccountPosted{AccountID: a.ID, EntryID: entryID, Side: r.Side, Amount: amount, Description: description, PostedAt: now})
	return nil
}

// Release drops a reservation without posting. Closed accounts may still
// release so that holds never get stuck.
func (a *Account) Release(holdID uuid.UUID) error {
	r, ok := a.Reservations[holdID]
	if !ok {
		return Errorf(CodeHoldNotPending, "no reservation for hold %s on account %s", holdID, a.ID)
	}
	a.raise(HoldReleased{AccountID: a.ID, HoldID: holdID, Side: r.Side, Amount: r.Amount})
	return nil
}

// Close marks the account closed. It must be empty and have no reservations.
func (a *Account) Close(now time.Time) error {
	if err := a.ensureOpen(); err != nil {
		return err
	}
	if a.Balance() != 0 || len(a.Reservations) > 0 {
		return Errorf(CodeAccountNotEmpty, "account %s has a non-zero balance or pending holds", a.ID)
	}
	a.raise(AccountClosed{AccountID: a.ID, ClosedAt: now})
	return nil
}

func (a *Account) checkTotals(side Side, amount int64) error {
	total := a.Debits
	if side == Credit {
		total = a.Credits
	}
	if _, err := money.Add(total, amount); err != nil {
		return Errorf(CodeValidation, "posting would overflow account totals")
	}
	return nil
}

func (a *Account) raise(e Event) {
	if err := a.Apply(e); err != nil {
		panic(fmt.Sprintf("account: applying freshly raised event: %v", err))
	}
	a.changes = append(a.changes, e)
}

// Apply mutates state from an event. It is used for both replay and raise.
func (a *Account) Apply(e Event) error {
	switch ev := e.(type) {
	case AccountOpened:
		a.ID, a.Name, a.Type, a.Currency, a.Policy = ev.AccountID, ev.Name, ev.Type, ev.Currency, ev.Policy
		a.Status, a.OpenedAt = StatusOpen, ev.OpenedAt
		a.Reservations = make(map[uuid.UUID]Reservation)
	case AccountClosed:
		a.Status = StatusClosed
	case AccountPosted:
		if ev.Side == Debit {
			a.Debits += ev.Amount
		} else {
			a.Credits += ev.Amount
		}
	case HoldReserved:
		a.Reservations[ev.HoldID] = Reservation{Side: ev.Side, Amount: ev.Amount, ExpiresAt: ev.ExpiresAt}
	case HoldReleased:
		delete(a.Reservations, ev.HoldID)
	default:
		return fmt.Errorf("account: unexpected event %T", e)
	}
	return nil
}

// Loaded marks the aggregate as rebuilt at version v with no pending changes.
func (a *Account) Loaded(v int64) {
	a.version = v
	a.changes = nil
	if a.Reservations == nil {
		a.Reservations = make(map[uuid.UUID]Reservation)
	}
}
