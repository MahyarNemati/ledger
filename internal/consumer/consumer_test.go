package consumer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/MahyarNemati/ledger/internal/domain"
	"github.com/MahyarNemati/ledger/internal/eventstore"
	"github.com/MahyarNemati/ledger/internal/kafka"
	"github.com/MahyarNemati/ledger/internal/ledger"
)

func TestRedeliveredMessagesApplyOnce(t *testing.T) {
	ctx := context.Background()
	store := eventstore.NewMemory()
	svc := ledger.NewService(store, ledger.Options{OutboxTopic: "ledger.events"})
	a, _ := svc.OpenAccount(ctx, ledger.OpenAccountCmd{Name: "a", Type: domain.Asset, Currency: "USD"})
	b, _ := svc.OpenAccount(ctx, ledger.OpenAccountCmd{Name: "b", Type: domain.Liability, Currency: "USD"})
	for i := 0; i < 3; i++ {
		if _, err := svc.Transfer(ctx, ledger.TransferCmd{DebitAccountID: a.ID, CreditAccountID: b.ID, Amount: 10, Currency: "USD"}); err != nil {
			t.Fatal(err)
		}
	}

	mem := NewMemory()
	h := &ActivityHandler{Name: "activity", Store: mem}
	deliver := func() {
		for _, m := range store.Outbox() {
			if err := h.Handle(ctx, kafka.Delivery{Topic: m.Topic, Key: []byte(m.Key), Value: m.Payload}); err != nil {
				t.Fatal(err)
			}
		}
	}
	deliver()
	deliver()

	act, _ := mem.Activity(ctx, a.ID)
	if act.Postings != 3 {
		t.Fatalf("account a has %d postings after redelivery, want 3", act.Postings)
	}
	act, _ = mem.Activity(ctx, b.ID)
	if act.Postings != 3 {
		t.Fatalf("account b has %d postings after redelivery, want 3", act.Postings)
	}
}

func TestPoisonMessagesAreSkipped(t *testing.T) {
	h := &ActivityHandler{Name: "activity", Store: NewMemory()}
	for _, v := range [][]byte{[]byte("not json"), mustJSON(ledger.Envelope{Type: "AccountPosted", Data: json.RawMessage(`"bad"`)})} {
		if err := h.Handle(context.Background(), kafka.Delivery{Value: v}); err != nil {
			t.Fatalf("poison message should be skipped, got %v", err)
		}
	}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
