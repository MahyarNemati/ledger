// Package consumer is an example downstream service. It keeps a per-account
// activity counter from ledger events and uses an inbox so that redelivered
// messages are applied exactly once.
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/MahyarNemati/ledger/internal/domain"
	"github.com/MahyarNemati/ledger/internal/kafka"
	"github.com/MahyarNemati/ledger/internal/ledger"
)

// Activity is the consumer's read model for one account.
type Activity struct {
	AccountID uuid.UUID
	Postings  int64
	LastEntry uuid.UUID
}

// Store applies a posting at most once per message ID.
type Store interface {
	ApplyPosting(ctx context.Context, consumer string, messageID uuid.UUID, accountID, entryID uuid.UUID) (applied bool, err error)
	Activity(ctx context.Context, accountID uuid.UUID) (Activity, error)
}

// ActivityHandler turns ledger envelopes into activity updates.
type ActivityHandler struct {
	Name  string
	Store Store
}

// Handle implements kafka.Handler.
func (h *ActivityHandler) Handle(ctx context.Context, d kafka.Delivery) error {
	var env ledger.Envelope
	if err := json.Unmarshal(d.Value, &env); err != nil {
		// Poison messages are skipped rather than blocking the partition.
		return nil
	}
	if env.Type != (domain.AccountPosted{}).EventType() {
		return nil
	}
	var posted domain.AccountPosted
	if err := json.Unmarshal(env.Data, &posted); err != nil {
		return nil
	}
	if _, err := h.Store.ApplyPosting(ctx, h.Name, env.EventID, posted.AccountID, posted.EntryID); err != nil {
		return fmt.Errorf("apply posting %s: %w", env.EventID, err)
	}
	return nil
}

// Memory is an in-process Store.
type Memory struct {
	mu       sync.Mutex
	inbox    map[string]time.Time
	activity map[uuid.UUID]Activity
}

func NewMemory() *Memory {
	return &Memory{inbox: map[string]time.Time{}, activity: map[uuid.UUID]Activity{}}
}

func (m *Memory) ApplyPosting(_ context.Context, consumer string, msgID, accountID, entryID uuid.UUID) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := consumer + "/" + msgID.String()
	if _, seen := m.inbox[k]; seen {
		return false, nil
	}
	m.inbox[k] = time.Now()
	a := m.activity[accountID]
	a.AccountID, a.Postings, a.LastEntry = accountID, a.Postings+1, entryID
	m.activity[accountID] = a
	return true, nil
}

func (m *Memory) Activity(_ context.Context, id uuid.UUID) (Activity, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activity[id], nil
}
