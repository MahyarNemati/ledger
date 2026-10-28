//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/MahyarNemati/ledger/internal/domain"
	"github.com/MahyarNemati/ledger/internal/eventstore"
	"github.com/MahyarNemati/ledger/internal/eventstore/storetest"
	"github.com/MahyarNemati/ledger/internal/idempotency"
	idemtest "github.com/MahyarNemati/ledger/internal/idempotency/storetest"
	"github.com/MahyarNemati/ledger/internal/ledger"
	"github.com/MahyarNemati/ledger/internal/outbox"
	"github.com/MahyarNemati/ledger/internal/postgres"
	"github.com/MahyarNemati/ledger/internal/postgres/pgtest"
	"github.com/MahyarNemati/ledger/internal/projection"
)

func TestEventStoreConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) eventstore.Store { return postgres.NewEventStore(pgtest.Pool(t)) })
}

func TestIdempotencyConformance(t *testing.T) {
	idemtest.Run(t, func(t *testing.T) idempotency.Store { return postgres.NewIdempotencyStore(pgtest.Pool(t)) })
}

func TestIdempotencyStaleClaimIsTakenOver(t *testing.T) {
	ctx := context.Background()
	s := postgres.NewIdempotencyStore(pgtest.Pool(t))
	s.StaleAfter = 50 * time.Millisecond
	h := idempotency.Hash("POST", "/v1/transfers", []byte(`{}`))
	if _, err := s.Begin(ctx, "transfers", "k", h); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Begin(ctx, "transfers", "k", h); !errors.Is(err, idempotency.ErrInProgress) {
		t.Fatalf("want in progress, got %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if rec, err := s.Begin(ctx, "transfers", "k", h); err != nil || rec != nil {
		t.Fatalf("stale claim not taken over: %v, %v", rec, err)
	}
}

func TestEventsAreAppendOnly(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.Pool(t)
	s := postgres.NewEventStore(pool)
	if _, err := s.Append(ctx, []eventstore.Append{{StreamID: "s-1", Events: []eventstore.NewEvent{{ID: uuid.New(), Type: "A", Data: json.RawMessage(`{}`)}}}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE events SET data = '{"x":1}'`); err == nil {
		t.Fatal("update of events must fail")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM events`); err == nil {
		t.Fatal("delete of events must fail")
	}
}

type recordingPublisher struct {
	mu   sync.Mutex
	fail bool
	got  []outbox.Message
}

func (p *recordingPublisher) Publish(_ context.Context, msgs []outbox.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail {
		return errors.New("broker down")
	}
	p.got = append(p.got, msgs...)
	return nil
}

func newService(t *testing.T) (*ledger.Service, *postgres.EventStore, *postgres.OutboxSource, *postgres.Projections) {
	pool := pgtest.Pool(t)
	store := postgres.NewEventStore(pool)
	svc := ledger.NewService(store, ledger.Options{SnapshotEvery: 5, OutboxTopic: "ledger.events"})
	return svc, store, postgres.NewOutboxSource(pool), postgres.NewProjections(pool)
}

func openPair(t *testing.T, svc *ledger.Service, funds int64) (settlement, customer uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	s, err := svc.OpenAccount(ctx, ledger.OpenAccountCmd{Name: "settlement", Type: domain.Asset, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	c, err := svc.OpenAccount(ctx, ledger.OpenAccountCmd{Name: "customer", Type: domain.Liability, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	if funds > 0 {
		if _, err := svc.Transfer(ctx, ledger.TransferCmd{DebitAccountID: s.ID, CreditAccountID: c.ID, Amount: funds, Currency: "USD"}); err != nil {
			t.Fatal(err)
		}
	}
	return s.ID, c.ID
}

func TestOutboxRelayOrderingAndRetry(t *testing.T) {
	ctx := context.Background()
	svc, _, src, _ := newService(t)
	openPair(t, svc, 1_000)

	pub := &recordingPublisher{fail: true}
	relay := &outbox.Relay{Source: src, Publisher: pub, Batch: 2}
	if _, err := relay.Step(ctx); err == nil {
		t.Fatal("expected publish failure")
	}
	if n, _ := src.Backlog(ctx); n == 0 {
		t.Fatal("failed batch must remain in the outbox")
	}
	pub.fail = false
	for {
		n, err := relay.Step(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	if n, _ := src.Backlog(ctx); n != 0 {
		t.Fatalf("backlog %d after drain", n)
	}
	// Two account openings, then the transfer: entry + two postings.
	if len(pub.got) != 5 {
		t.Fatalf("published %d messages, want 5", len(pub.got))
	}
	for i := 1; i < len(pub.got); i++ {
		if pub.got[i].Seq <= pub.got[i-1].Seq {
			t.Fatal("messages out of order")
		}
	}
	var env ledger.Envelope
	if err := json.Unmarshal(pub.got[2].Payload, &env); err != nil || env.Type != "EntryPosted" {
		t.Fatalf("unexpected envelope %+v (%v)", env, err)
	}
}

func TestOnlyOneRelayIsActive(t *testing.T) {
	ctx := context.Background()
	svc, _, src, _ := newService(t)
	openPair(t, svc, 0)

	entered := make(chan struct{})
	release := make(chan struct{})
	blocking := outbox.Relay{Source: src, Publisher: publisherFunc(func(context.Context, []outbox.Message) error {
		close(entered)
		<-release
		return nil
	})}
	done := make(chan error, 1)
	go func() { _, err := blocking.Step(ctx); done <- err }()
	<-entered

	other := &recordingPublisher{}
	n, err := (&outbox.Relay{Source: src, Publisher: other}).Step(ctx)
	if err != nil || n != 0 || len(other.got) != 0 {
		t.Fatalf("second relay should stand by, got n=%d err=%v", n, err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type publisherFunc func(context.Context, []outbox.Message) error

func (f publisherFunc) Publish(ctx context.Context, m []outbox.Message) error { return f(ctx, m) }

func TestConcurrentTransfersOnPostgres(t *testing.T) {
	ctx := context.Background()
	svc, store, _, proj := newService(t)
	settlement, customer := openPair(t, svc, 1_000)
	merchant, err := svc.OpenAccount(ctx, ledger.OpenAccountCmd{Name: "merchant", Type: domain.Liability, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}

	const workers, each = 8, 10
	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded := 0
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				_, err := svc.Transfer(ctx, ledger.TransferCmd{DebitAccountID: customer, CreditAccountID: merchant.ID, Amount: 25, Currency: "USD"})
				switch {
				case err == nil:
					mu.Lock()
					succeeded++
					mu.Unlock()
				case domain.IsCode(err, domain.CodeInsufficientFunds), domain.IsCode(err, domain.CodeConflict):
				default:
					t.Errorf("transfer: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	c, err := svc.GetAccount(ctx, customer)
	if err != nil {
		t.Fatal(err)
	}
	if c.Balance() < 0 {
		t.Fatalf("customer overdrawn: %d", c.Balance())
	}
	if c.Balance() != 1_000-int64(succeeded)*25 {
		t.Fatalf("balance %d does not match %d successful transfers", c.Balance(), succeeded)
	}

	runner := &projection.Runner{Name: "balances", Store: store, Backend: proj, Batch: 50}
	ids := []uuid.UUID{settlement, customer, merchant.ID}
	pgtest.Eventually(t, 10*time.Second, func() error {
		if err := runner.CatchUp(ctx); err != nil {
			return err
		}
		var sum int64
		for _, id := range ids {
			a, err := svc.GetAccount(ctx, id)
			if err != nil {
				return err
			}
			b, err := proj.Balance(ctx, id)
			if err != nil {
				return err
			}
			if b.Debits != a.Debits || b.Credits != a.Credits {
				return fmt.Errorf("projection behind for %s", id)
			}
			sum += b.Debits - b.Credits
		}
		if sum != 0 {
			return fmt.Errorf("projected balances sum to %d", sum)
		}
		return nil
	})

	lines, err := proj.Statement(ctx, customer, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != succeeded+1 || lines[len(lines)-1].BalanceAfter != c.Balance() {
		t.Fatalf("statement has %d lines ending at %d", len(lines), lines[len(lines)-1].BalanceAfter)
	}
}

func TestSnapshotsRoundTripOnPostgres(t *testing.T) {
	ctx := context.Background()
	svc, _, _, _ := newService(t)
	settlement, customer := openPair(t, svc, 500)
	for i := 0; i < 12; i++ {
		if _, err := svc.Transfer(ctx, ledger.TransferCmd{DebitAccountID: customer, CreditAccountID: settlement, Amount: 10, Currency: "USD"}); err != nil {
			t.Fatal(err)
		}
	}
	snapped, err := svc.GetAccount(ctx, customer)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := svc.Repository().ReplayAccount(ctx, customer)
	if err != nil {
		t.Fatal(err)
	}
	if snapped.Debits != replayed.Debits || snapped.Credits != replayed.Credits || snapped.Version() != replayed.Version() {
		t.Fatalf("snapshot %d/%d v%d, replay %d/%d v%d", snapped.Debits, snapped.Credits, snapped.Version(),
			replayed.Debits, replayed.Credits, replayed.Version())
	}
}

func TestInboxDedupesConcurrentRedelivery(t *testing.T) {
	ctx := context.Background()
	store := postgres.NewActivityStore(pgtest.Pool(t))
	msg, account, entry := uuid.New(), uuid.New(), uuid.New()

	var wg sync.WaitGroup
	var mu sync.Mutex
	applied := 0
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := store.ApplyPosting(ctx, "activity", msg, account, entry)
			if err != nil {
				t.Error(err)
				return
			}
			if ok {
				mu.Lock()
				applied++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if applied != 1 {
		t.Fatalf("message applied %d times", applied)
	}
	act, err := store.Activity(ctx, account)
	if err != nil || act.Postings != 1 || act.LastEntry != entry {
		t.Fatalf("activity = %+v, %v", act, err)
	}
	if ok, _ := store.ApplyPosting(ctx, "other-consumer", msg, account, entry); !ok {
		t.Fatal("inbox must be scoped per consumer")
	}
}
