package ledger

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"pgregory.net/rapid"

	"github.com/MahyarNemati/ledger/internal/domain"
	"github.com/MahyarNemati/ledger/internal/eventstore"
	"github.com/MahyarNemati/ledger/internal/projection"
)

// ledgerMachine drives random command sequences against the service and
// checks the ledger invariants after every step.
type ledgerMachine struct {
	ctx      context.Context
	store    *eventstore.Memory
	svc      *Service
	clock    *fakeClock
	accounts []uuid.UUID
	entries  []uuid.UUID
	holds    []uuid.UUID
	proj     *projection.Memory
	runner   *projection.Runner
}

var acceptable = map[domain.Code]bool{
	domain.CodeInsufficientFunds: true,
	domain.CodeCurrencyMismatch:  true,
	domain.CodeAccountClosed:     true,
	domain.CodeAccountNotEmpty:   true,
	domain.CodeAlreadyReversed:   true,
	domain.CodeNotReversible:     true,
	domain.CodeHoldNotPending:    true,
	domain.CodeHoldExpired:       true,
	domain.CodeValidation:        true,
}

func newMachine(t *rapid.T) *ledgerMachine {
	store := eventstore.NewMemory()
	clock := newClock()
	every := rapid.SampledFrom([]int64{0, 1, 3, 16}).Draw(t, "snapshotEvery")
	m := &ledgerMachine{
		ctx:   context.Background(),
		store: store,
		clock: clock,
		svc:   NewService(store, Options{SnapshotEvery: every, Now: clock.Now}),
		proj:  projection.NewMemory(),
	}
	m.runner = &projection.Runner{Name: "balances", Store: store, Backend: m.proj, Batch: 7}
	return m
}

func (m *ledgerMachine) check(t *rapid.T, err error) {
	if err == nil {
		return
	}
	if !acceptable[domain.CodeOf(err)] {
		t.Fatalf("unexpected error: %v", err)
	}
}

func (m *ledgerMachine) pickAccount(t *rapid.T, label string) uuid.UUID {
	return rapid.SampledFrom(m.accounts).Draw(t, label)
}

func (m *ledgerMachine) pickPair(t *rapid.T) (uuid.UUID, uuid.UUID) {
	ids := rapid.SliceOfNDistinct(rapid.SampledFrom(m.accounts), 2, 2, func(id uuid.UUID) string { return id.String() }).Draw(t, "pair")
	return ids[0], ids[1]
}

func (m *ledgerMachine) Open(t *rapid.T) {
	typ := rapid.SampledFrom([]domain.AccountType{domain.Asset, domain.Liability, domain.Equity, domain.Revenue, domain.Expense}).Draw(t, "type")
	cur := rapid.SampledFrom([]string{"USD", "JPY"}).Draw(t, "currency")
	p := domain.Policy{
		AllowOverdraft: rapid.Float64Range(0, 1).Draw(t, "overdraftRoll") < 0.3,
		OverdraftLimit: rapid.Int64Range(0, 2_000).Draw(t, "limit"),
	}
	a, err := m.svc.OpenAccount(m.ctx, OpenAccountCmd{Name: "acct", Type: typ, Currency: cur, Policy: p})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	m.accounts = append(m.accounts, a.ID)
}

func (m *ledgerMachine) Transfer(t *rapid.T) {
	if len(m.accounts) < 2 {
		t.Skip("need two accounts")
	}
	d, c := m.pickPair(t)
	cur := rapid.SampledFrom([]string{"USD", "JPY"}).Draw(t, "currency")
	e, err := m.svc.Transfer(m.ctx, TransferCmd{DebitAccountID: d, CreditAccountID: c, Amount: rapid.Int64Range(1, 5_000).Draw(t, "amount"), Currency: cur})
	m.check(t, err)
	if err == nil {
		m.entries = append(m.entries, e.ID)
	}
}

func (m *ledgerMachine) MultiLineEntry(t *rapid.T) {
	if len(m.accounts) < 3 {
		t.Skip("need three accounts")
	}
	n := rapid.IntRange(3, 5).Draw(t, "lines")
	ids := rapid.SliceOfNDistinct(rapid.SampledFrom(m.accounts), n, n, func(id uuid.UUID) string { return id.String() }).Draw(t, "accounts")
	total := rapid.Int64Range(int64(n), 20_000).Draw(t, "total")
	// One side gets a single line for the total, the other side splits it.
	lines := []domain.Line{{AccountID: ids[0], Side: domain.Debit, Amount: total}}
	remaining := total
	for i := 1; i < n; i++ {
		amt := remaining
		if i < n-1 {
			amt = rapid.Int64Range(1, remaining-int64(n-1-i)).Draw(t, "split")
		}
		remaining -= amt
		lines = append(lines, domain.Line{AccountID: ids[i], Side: domain.Credit, Amount: amt})
	}
	if rapid.Bool().Draw(t, "flip") {
		for i := range lines {
			lines[i].Side = lines[i].Side.Opposite()
		}
	}
	cur := rapid.SampledFrom([]string{"USD", "JPY"}).Draw(t, "currency")
	e, err := m.svc.PostEntry(m.ctx, PostEntryCmd{Currency: cur, Lines: lines})
	m.check(t, err)
	if err == nil {
		m.entries = append(m.entries, e.ID)
	}
}

func (m *ledgerMachine) Reverse(t *rapid.T) {
	if len(m.entries) == 0 {
		t.Skip("no entries")
	}
	id := rapid.SampledFrom(m.entries).Draw(t, "entry")
	e, err := m.svc.Reverse(m.ctx, ReverseCmd{EntryID: id})
	m.check(t, err)
	if err == nil {
		m.entries = append(m.entries, e.ID)
	}
}

func (m *ledgerMachine) PlaceHold(t *rapid.T) {
	if len(m.accounts) < 2 {
		t.Skip("need two accounts")
	}
	d, c := m.pickPair(t)
	cur := rapid.SampledFrom([]string{"USD", "JPY"}).Draw(t, "currency")
	ttl := time.Duration(rapid.IntRange(1, 120).Draw(t, "ttlMinutes")) * time.Minute
	h, err := m.svc.PlaceHold(m.ctx, PlaceHoldCmd{
		DebitAccountID: d, CreditAccountID: c, Currency: cur,
		Amount: rapid.Int64Range(1, 5_000).Draw(t, "amount"), ExpiresAt: m.clock.Now().Add(ttl),
	})
	m.check(t, err)
	if err == nil {
		m.holds = append(m.holds, h.ID)
	}
}

func (m *ledgerMachine) CaptureHold(t *rapid.T) {
	if len(m.holds) == 0 {
		t.Skip("no holds")
	}
	id := rapid.SampledFrom(m.holds).Draw(t, "hold")
	res, err := m.svc.CaptureHold(m.ctx, CaptureHoldCmd{HoldID: id, Amount: rapid.Int64Range(0, 6_000).Draw(t, "amount")})
	m.check(t, err)
	if err == nil {
		m.entries = append(m.entries, res.Entry.ID)
	}
}

func (m *ledgerMachine) VoidHold(t *rapid.T) {
	if len(m.holds) == 0 {
		t.Skip("no holds")
	}
	_, err := m.svc.VoidHold(m.ctx, rapid.SampledFrom(m.holds).Draw(t, "hold"), "")
	m.check(t, err)
}

func (m *ledgerMachine) Close(t *rapid.T) {
	if len(m.accounts) == 0 {
		t.Skip("no accounts")
	}
	_, err := m.svc.CloseAccount(m.ctx, m.pickAccount(t, "account"))
	m.check(t, err)
}

func (m *ledgerMachine) Tick(t *rapid.T) {
	m.clock.Advance(time.Duration(rapid.IntRange(1, 90).Draw(t, "minutes")) * time.Minute)
}

func (m *ledgerMachine) Check(t *rapid.T) {
	now := m.clock.Now()
	type sums struct{ balance, pendingDebit, pendingCredit int64 }
	byCurrency := map[string]*sums{}
	accounts := map[uuid.UUID]*domain.Account{}

	for _, id := range m.accounts {
		a, err := m.svc.GetAccount(m.ctx, id)
		if err != nil {
			t.Fatalf("load %s: %v", id, err)
		}
		accounts[id] = a
		s := byCurrency[a.Currency]
		if s == nil {
			s = &sums{}
			byCurrency[a.Currency] = s
		}
		s.balance += a.SignedBalance()
		s.pendingDebit += a.Pending(domain.Debit)
		s.pendingCredit += a.Pending(domain.Credit)

		// No overdraft unless allowed.
		if !a.Policy.AllowOverdraft && a.Available(now) < -a.Policy.OverdraftLimit {
			t.Fatalf("account %s (%s) available %d below limit %d", id, a.Type, a.Available(now), a.Policy.OverdraftLimit)
		}

		// Replay from the full history yields the same state as snapshot+tail.
		replayed, err := m.svc.Repository().ReplayAccount(m.ctx, id)
		if err != nil {
			t.Fatalf("replay %s: %v", id, err)
		}
		if !reflect.DeepEqual(stateOf(a), stateOf(replayed)) || a.Version() != replayed.Version() {
			t.Fatalf("replay mismatch for %s:\n snapshot %+v\n replay   %+v", id, stateOf(a), stateOf(replayed))
		}
	}

	// The sum of all balances is zero, per currency, for posted and pending.
	for cur, s := range byCurrency {
		if s.balance != 0 {
			t.Fatalf("%s: sum of balances = %d", cur, s.balance)
		}
		if s.pendingDebit != s.pendingCredit {
			t.Fatalf("%s: pending debits %d != pending credits %d", cur, s.pendingDebit, s.pendingCredit)
		}
	}

	// Account totals equal the sum of journal lines that touch them.
	totals := map[uuid.UUID][2]int64{}
	for _, id := range m.entries {
		e, err := m.svc.GetEntry(m.ctx, id)
		if err != nil {
			t.Fatalf("entry %s: %v", id, err)
		}
		for _, l := range e.Lines {
			tt := totals[l.AccountID]
			if l.Side == domain.Debit {
				tt[0] += l.Amount
			} else {
				tt[1] += l.Amount
			}
			totals[l.AccountID] = tt
		}
	}
	for id, a := range accounts {
		if tt := totals[id]; tt[0] != a.Debits || tt[1] != a.Credits {
			t.Fatalf("account %s totals %d/%d, journal says %d/%d", id, a.Debits, a.Credits, tt[0], tt[1])
		}
	}

	// The asynchronous projection converges to the aggregate state.
	if err := m.runner.CatchUp(m.ctx); err != nil {
		t.Fatalf("projection: %v", err)
	}
	for id, a := range accounts {
		b, err := m.proj.Balance(m.ctx, id)
		if err != nil {
			t.Fatalf("projected balance %s: %v", id, err)
		}
		if b.Debits != a.Debits || b.Credits != a.Credits ||
			b.PendingDebits != a.Pending(domain.Debit) || b.PendingCredits != a.Pending(domain.Credit) {
			t.Fatalf("projection drift for %s: %+v vs aggregate %d/%d", id, b, a.Debits, a.Credits)
		}
	}
}

type accountState struct {
	Debits, Credits int64
	Status          domain.AccountStatus
	Reservations    map[uuid.UUID]domain.Reservation
}

func stateOf(a *domain.Account) accountState {
	r := make(map[uuid.UUID]domain.Reservation, len(a.Reservations))
	for k, v := range a.Reservations {
		v.ExpiresAt = v.ExpiresAt.UTC()
		r[k] = v
	}
	return accountState{Debits: a.Debits, Credits: a.Credits, Status: a.Status, Reservations: r}
}

func TestLedgerInvariants(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := newMachine(t)
		t.Repeat(rapid.StateMachineActions(m))
	})
}

// TestReplayIntoFreshStoreIsIdentical copies the global log into a new store
// and checks every aggregate rebuilds to the same state.
func TestReplayIntoFreshStoreIsIdentical(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := newMachine(t)
		for i := 0; i < 4; i++ {
			m.Open(t)
		}
		steps := rapid.IntRange(10, 60).Draw(t, "steps")
		for i := 0; i < steps; i++ {
			switch rapid.IntRange(0, 4).Draw(t, "op") {
			case 0:
				m.Transfer(t)
			case 1:
				m.PlaceHold(t)
			case 2:
				if len(m.holds) > 0 {
					m.CaptureHold(t)
				}
			case 3:
				if len(m.entries) > 0 {
					m.Reverse(t)
				}
			case 4:
				m.Tick(t)
			}
		}

		all, err := m.store.ReadAll(m.ctx, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		fresh := eventstore.NewMemory()
		versions := map[string]int64{}
		for _, ev := range all {
			_, err := fresh.Append(m.ctx, []eventstore.Append{{
				StreamID: ev.StreamID, ExpectedVersion: versions[ev.StreamID],
				Events: []eventstore.NewEvent{{ID: ev.ID, Type: ev.Type, Data: ev.Data}},
			}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			versions[ev.StreamID]++
		}
		replayRepo := NewRepository(fresh, 0, nil)
		for _, id := range m.accounts {
			orig, err := m.svc.GetAccount(m.ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			again, err := replayRepo.Account(m.ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(stateOf(orig), stateOf(again)) {
				t.Fatalf("fresh replay differs for %s", id)
			}
		}
	})
}
