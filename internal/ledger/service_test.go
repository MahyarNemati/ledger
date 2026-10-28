package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/MahyarNemati/ledger/internal/domain"
	"github.com/MahyarNemati/ledger/internal/eventstore"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type fixture struct {
	t     *testing.T
	ctx   context.Context
	store *eventstore.Memory
	svc   *Service
	clock *fakeClock
}

func newFixture(t *testing.T, snapshotEvery int64) *fixture {
	t.Helper()
	store := eventstore.NewMemory()
	clock := newClock()
	svc := NewService(store, Options{SnapshotEvery: snapshotEvery, OutboxTopic: "ledger.events", Now: clock.Now})
	return &fixture{t: t, ctx: context.Background(), store: store, svc: svc, clock: clock}
}

func (f *fixture) open(name string, typ domain.AccountType, cur string, p domain.Policy) *domain.Account {
	f.t.Helper()
	a, err := f.svc.OpenAccount(f.ctx, OpenAccountCmd{Name: name, Type: typ, Currency: cur, Policy: p})
	if err != nil {
		f.t.Fatalf("open %s: %v", name, err)
	}
	return a
}

func (f *fixture) account(id uuid.UUID) *domain.Account {
	f.t.Helper()
	a, err := f.svc.GetAccount(f.ctx, id)
	if err != nil {
		f.t.Fatalf("get account: %v", err)
	}
	return a
}

func wantCode(t *testing.T, err error, code domain.Code) {
	t.Helper()
	if !domain.IsCode(err, code) {
		t.Fatalf("want %s, got %v", code, err)
	}
}

// fund deposits amount into a customer liability account from a settlement asset.
func (f *fixture) fund(settlement, customer uuid.UUID, amount int64, cur string) {
	f.t.Helper()
	if _, err := f.svc.Transfer(f.ctx, TransferCmd{DebitAccountID: settlement, CreditAccountID: customer, Amount: amount, Currency: cur}); err != nil {
		f.t.Fatalf("fund: %v", err)
	}
}

func TestTransferAndOverdraft(t *testing.T) {
	f := newFixture(t, 0)
	cash := f.open("cash", domain.Asset, "USD", domain.Policy{AllowOverdraft: true})
	alice := f.open("alice", domain.Liability, "USD", domain.Policy{})
	bob := f.open("bob", domain.Liability, "USD", domain.Policy{OverdraftLimit: 500})

	f.fund(cash.ID, alice.ID, 10_000, "USD")

	if _, err := f.svc.Transfer(f.ctx, TransferCmd{DebitAccountID: alice.ID, CreditAccountID: bob.ID, Amount: 2_500, Currency: "USD"}); err != nil {
		t.Fatal(err)
	}
	if got := f.account(alice.ID).Balance(); got != 7_500 {
		t.Fatalf("alice balance = %d", got)
	}
	if got := f.account(cash.ID).Balance(); got != 10_000 {
		t.Fatalf("cash balance = %d", got)
	}

	_, err := f.svc.Transfer(f.ctx, TransferCmd{DebitAccountID: alice.ID, CreditAccountID: bob.ID, Amount: 7_501, Currency: "USD"})
	wantCode(t, err, domain.CodeInsufficientFunds)

	// bob may go 500 below zero.
	if _, err := f.svc.Transfer(f.ctx, TransferCmd{DebitAccountID: bob.ID, CreditAccountID: alice.ID, Amount: 3_000, Currency: "USD"}); err != nil {
		t.Fatal(err)
	}
	_, err = f.svc.Transfer(f.ctx, TransferCmd{DebitAccountID: bob.ID, CreditAccountID: alice.ID, Amount: 1, Currency: "USD"})
	wantCode(t, err, domain.CodeInsufficientFunds)
}

func TestEntryValidation(t *testing.T) {
	f := newFixture(t, 0)
	a := f.open("a", domain.Asset, "USD", domain.Policy{AllowOverdraft: true})
	b := f.open("b", domain.Equity, "USD", domain.Policy{AllowOverdraft: true})
	e := f.open("e", domain.Equity, "EUR", domain.Policy{AllowOverdraft: true})

	_, err := f.svc.PostEntry(f.ctx, PostEntryCmd{Currency: "USD", Lines: []domain.Line{
		{AccountID: a.ID, Side: domain.Debit, Amount: 100},
		{AccountID: b.ID, Side: domain.Credit, Amount: 99},
	}})
	wantCode(t, err, domain.CodeUnbalanced)

	_, err = f.svc.PostEntry(f.ctx, PostEntryCmd{Currency: "USD", Lines: []domain.Line{
		{AccountID: a.ID, Side: domain.Debit, Amount: 100},
		{AccountID: e.ID, Side: domain.Credit, Amount: 100},
	}})
	wantCode(t, err, domain.CodeCurrencyMismatch)

	_, err = f.svc.PostEntry(f.ctx, PostEntryCmd{Currency: "USD", Lines: []domain.Line{
		{AccountID: a.ID, Side: domain.Debit, Amount: 100},
		{AccountID: uuid.New(), Side: domain.Credit, Amount: 100},
	}})
	wantCode(t, err, domain.CodeNotFound)

	_, err = f.svc.PostEntry(f.ctx, PostEntryCmd{Kind: domain.KindReversal, Currency: "USD", Lines: []domain.Line{
		{AccountID: a.ID, Side: domain.Debit, Amount: 1},
		{AccountID: b.ID, Side: domain.Credit, Amount: 1},
	}})
	wantCode(t, err, domain.CodeValidation)

	// Nothing above may have produced events beyond the three openings.
	all, _ := f.store.ReadAll(f.ctx, 0, 0)
	if len(all) != 3 {
		t.Fatalf("expected 3 events, got %d", len(all))
	}
}

func TestDuplicateEntryIDIsRejected(t *testing.T) {
	f := newFixture(t, 0)
	a := f.open("a", domain.Asset, "USD", domain.Policy{AllowOverdraft: true})
	b := f.open("b", domain.Equity, "USD", domain.Policy{AllowOverdraft: true})
	id := uuid.New()
	cmd := TransferCmd{ID: id, DebitAccountID: a.ID, CreditAccountID: b.ID, Amount: 10, Currency: "USD"}
	if _, err := f.svc.Transfer(f.ctx, cmd); err != nil {
		t.Fatal(err)
	}
	_, err := f.svc.Transfer(f.ctx, cmd)
	wantCode(t, err, domain.CodeAlreadyExists)
	if got := f.account(a.ID).Balance(); got != 10 {
		t.Fatalf("duplicate was applied, balance %d", got)
	}
}

func TestReversal(t *testing.T) {
	f := newFixture(t, 0)
	cash := f.open("cash", domain.Asset, "USD", domain.Policy{AllowOverdraft: true})
	alice := f.open("alice", domain.Liability, "USD", domain.Policy{})
	bob := f.open("bob", domain.Liability, "USD", domain.Policy{})
	f.fund(cash.ID, alice.ID, 1_000, "USD")

	tr, err := f.svc.Transfer(f.ctx, TransferCmd{DebitAccountID: alice.ID, CreditAccountID: bob.ID, Amount: 400, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	rev, err := f.svc.Reverse(f.ctx, ReverseCmd{EntryID: tr.ID, Reason: "customer dispute"})
	if err != nil {
		t.Fatal(err)
	}
	if rev.Reverses == nil || *rev.Reverses != tr.ID || rev.Kind != domain.KindReversal {
		t.Fatalf("bad reversal linkage: %+v", rev)
	}
	if f.account(alice.ID).Balance() != 1_000 || f.account(bob.ID).Balance() != 0 {
		t.Fatal("reversal did not restore balances")
	}
	orig, _ := f.svc.GetEntry(f.ctx, tr.ID)
	if orig.ReversedBy == nil || *orig.ReversedBy != rev.ID {
		t.Fatal("original not marked reversed")
	}
	// History is preserved: the original posting is still on the stream.
	evs, _ := f.store.Load(f.ctx, domain.AccountStream(bob.ID), 0)
	if len(evs) != 3 {
		t.Fatalf("bob stream has %d events, want 3", len(evs))
	}

	_, err = f.svc.Reverse(f.ctx, ReverseCmd{EntryID: tr.ID})
	wantCode(t, err, domain.CodeAlreadyReversed)
	_, err = f.svc.Reverse(f.ctx, ReverseCmd{EntryID: rev.ID})
	wantCode(t, err, domain.CodeNotReversible)
}

func TestReversalRespectsFunds(t *testing.T) {
	f := newFixture(t, 0)
	cash := f.open("cash", domain.Asset, "USD", domain.Policy{AllowOverdraft: true})
	alice := f.open("alice", domain.Liability, "USD", domain.Policy{})
	bob := f.open("bob", domain.Liability, "USD", domain.Policy{})
	f.fund(cash.ID, alice.ID, 500, "USD")
	tr, _ := f.svc.Transfer(f.ctx, TransferCmd{DebitAccountID: alice.ID, CreditAccountID: bob.ID, Amount: 500, Currency: "USD"})
	if _, err := f.svc.Transfer(f.ctx, TransferCmd{DebitAccountID: bob.ID, CreditAccountID: alice.ID, Amount: 200, Currency: "USD"}); err != nil {
		t.Fatal(err)
	}
	_, err := f.svc.Reverse(f.ctx, ReverseCmd{EntryID: tr.ID})
	wantCode(t, err, domain.CodeInsufficientFunds)
}

func TestHoldLifecycle(t *testing.T) {
	f := newFixture(t, 0)
	cash := f.open("cash", domain.Asset, "USD", domain.Policy{AllowOverdraft: true})
	card := f.open("cardholder", domain.Liability, "USD", domain.Policy{})
	merchant := f.open("merchant", domain.Liability, "USD", domain.Policy{})
	f.fund(cash.ID, card.ID, 10_000, "USD")

	exp := f.clock.Now().Add(time.Hour)
	h, err := f.svc.PlaceHold(f.ctx, PlaceHoldCmd{DebitAccountID: card.ID, CreditAccountID: merchant.ID, Amount: 6_000, Currency: "USD", ExpiresAt: exp})
	if err != nil {
		t.Fatal(err)
	}
	c := f.account(card.ID)
	if c.Balance() != 10_000 || c.Available(f.clock.Now()) != 4_000 {
		t.Fatalf("balance %d available %d", c.Balance(), c.Available(f.clock.Now()))
	}
	_, err = f.svc.Transfer(f.ctx, TransferCmd{DebitAccountID: card.ID, CreditAccountID: merchant.ID, Amount: 4_001, Currency: "USD"})
	wantCode(t, err, domain.CodeInsufficientFunds)

	_, err = f.svc.CaptureHold(f.ctx, CaptureHoldCmd{HoldID: h.ID, Amount: 6_001})
	wantCode(t, err, domain.CodeValidation)

	res, err := f.svc.CaptureHold(f.ctx, CaptureHoldCmd{HoldID: h.ID, Amount: 5_000})
	if err != nil {
		t.Fatal(err)
	}
	if res.Hold.Status != domain.HoldStatusCaptured || res.Entry.Kind != domain.KindCapture {
		t.Fatalf("unexpected capture result %+v", res)
	}
	c = f.account(card.ID)
	if c.Balance() != 5_000 || c.Available(f.clock.Now()) != 5_000 || len(c.Reservations) != 0 {
		t.Fatalf("after capture balance %d available %d", c.Balance(), c.Available(f.clock.Now()))
	}
	if f.account(merchant.ID).Balance() != 5_000 {
		t.Fatal("merchant not credited")
	}
	_, err = f.svc.CaptureHold(f.ctx, CaptureHoldCmd{HoldID: h.ID})
	wantCode(t, err, domain.CodeHoldNotPending)
	_, err = f.svc.VoidHold(f.ctx, h.ID, "")
	wantCode(t, err, domain.CodeHoldNotPending)
}

func TestHoldExpiryAndVoid(t *testing.T) {
	f := newFixture(t, 0)
	cash := f.open("cash", domain.Asset, "USD", domain.Policy{AllowOverdraft: true})
	card := f.open("cardholder", domain.Liability, "USD", domain.Policy{})
	merchant := f.open("merchant", domain.Liability, "USD", domain.Policy{})
	f.fund(cash.ID, card.ID, 1_000, "USD")

	h, err := f.svc.PlaceHold(f.ctx, PlaceHoldCmd{DebitAccountID: card.ID, CreditAccountID: merchant.ID, Amount: 1_000, Currency: "USD", ExpiresAt: f.clock.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(2 * time.Minute)
	if got := f.account(card.ID).Available(f.clock.Now()); got != 1_000 {
		t.Fatalf("expired hold still counted, available %d", got)
	}
	_, err = f.svc.CaptureHold(f.ctx, CaptureHoldCmd{HoldID: h.ID})
	wantCode(t, err, domain.CodeHoldExpired)
	v, err := f.svc.VoidHold(f.ctx, h.ID, "expired")
	if err != nil {
		t.Fatal(err)
	}
	if v.Status != domain.HoldStatusVoided || len(f.account(card.ID).Reservations) != 0 || len(f.account(merchant.ID).Reservations) != 0 {
		t.Fatal("void did not release reservations")
	}
}

func TestCloseAccount(t *testing.T) {
	f := newFixture(t, 0)
	cash := f.open("cash", domain.Asset, "USD", domain.Policy{AllowOverdraft: true})
	alice := f.open("alice", domain.Liability, "USD", domain.Policy{})
	f.fund(cash.ID, alice.ID, 10, "USD")
	_, err := f.svc.CloseAccount(f.ctx, alice.ID)
	wantCode(t, err, domain.CodeAccountNotEmpty)
	if _, err := f.svc.Transfer(f.ctx, TransferCmd{DebitAccountID: alice.ID, CreditAccountID: cash.ID, Amount: 10, Currency: "USD"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.CloseAccount(f.ctx, alice.ID); err != nil {
		t.Fatal(err)
	}
	_, err = f.svc.Transfer(f.ctx, TransferCmd{DebitAccountID: cash.ID, CreditAccountID: alice.ID, Amount: 1, Currency: "USD"})
	wantCode(t, err, domain.CodeAccountClosed)
}

func TestOutboxEnvelopes(t *testing.T) {
	f := newFixture(t, 0)
	a := f.open("a", domain.Asset, "USD", domain.Policy{AllowOverdraft: true})
	b := f.open("b", domain.Equity, "USD", domain.Policy{AllowOverdraft: true})
	if _, err := f.svc.Transfer(f.ctx, TransferCmd{DebitAccountID: a.ID, CreditAccountID: b.ID, Amount: 5, Currency: "USD"}); err != nil {
		t.Fatal(err)
	}
	msgs := f.store.Outbox()
	all, _ := f.store.ReadAll(f.ctx, 0, 0)
	if len(msgs) != len(all) {
		t.Fatalf("outbox has %d messages for %d events", len(msgs), len(all))
	}
	for i, m := range msgs {
		var env Envelope
		if err := json.Unmarshal(m.Payload, &env); err != nil {
			t.Fatal(err)
		}
		if env.EventID != all[i].ID || env.Type != all[i].Type || env.Version != all[i].Version || m.Key != all[i].StreamID {
			t.Fatalf("envelope %d does not match event: %+v vs %+v", i, env, all[i])
		}
		if m.Headers["message-id"] != all[i].ID.String() {
			t.Fatal("missing message-id header")
		}
	}
}

func TestSnapshotsMatchReplay(t *testing.T) {
	f := newFixture(t, 3)
	cash := f.open("cash", domain.Asset, "USD", domain.Policy{AllowOverdraft: true})
	alice := f.open("alice", domain.Liability, "USD", domain.Policy{})
	for i := 0; i < 10; i++ {
		f.fund(cash.ID, alice.ID, int64(i+1), "USD")
	}
	snap, err := f.store.LoadSnapshot(f.ctx, domain.AccountStream(alice.ID))
	if err != nil {
		t.Fatalf("expected a snapshot: %v", err)
	}
	if snap.Version != 9 {
		t.Fatalf("snapshot version = %d, want 9", snap.Version)
	}
	fromSnap := f.account(alice.ID)
	replayed, err := f.svc.Repository().ReplayAccount(f.ctx, alice.ID)
	if err != nil {
		t.Fatal(err)
	}
	if fromSnap.Balance() != 55 || replayed.Balance() != 55 || fromSnap.Version() != replayed.Version() {
		t.Fatalf("snapshot %d@%d replay %d@%d", fromSnap.Balance(), fromSnap.Version(), replayed.Balance(), replayed.Version())
	}
}

func TestConcurrentTransfersNeverOverdraw(t *testing.T) {
	f := newFixture(t, 5)
	f.svc.maxRetries = 1000
	cash := f.open("cash", domain.Asset, "USD", domain.Policy{AllowOverdraft: true})
	src := f.open("src", domain.Liability, "USD", domain.Policy{})
	dst := f.open("dst", domain.Liability, "USD", domain.Policy{})
	f.fund(cash.ID, src.ID, 1_000, "USD")

	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, insufficient := 0, 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.svc.Transfer(f.ctx, TransferCmd{DebitAccountID: src.ID, CreditAccountID: dst.ID, Amount: 30, Currency: "USD"})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case domain.IsCode(err, domain.CodeInsufficientFunds):
				insufficient++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	if ok != 33 || insufficient != 17 {
		t.Fatalf("ok=%d insufficient=%d", ok, insufficient)
	}
	if got := f.account(src.ID).Balance(); got != 10 {
		t.Fatalf("src balance = %d", got)
	}
}

func TestConflictErrorUnwraps(t *testing.T) {
	err := error(&eventstore.ConflictError{StreamID: "x"})
	if !errors.Is(err, eventstore.ErrConcurrency) {
		t.Fatal("ConflictError must unwrap to ErrConcurrency")
	}
}
