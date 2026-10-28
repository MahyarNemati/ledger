package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/MahyarNemati/ledger/internal/domain"
	"github.com/MahyarNemati/ledger/internal/eventstore"
)

type aggregate interface {
	Apply(domain.Event) error
	Loaded(version int64)
	Version() int64
	Changes() []domain.Event
}

// Repository rebuilds aggregates from snapshots plus subsequent events.
type Repository struct {
	store         eventstore.Store
	snapshotEvery int64
	log           *slog.Logger
}

// NewRepository returns a repository. snapshotEvery <= 0 disables snapshots.
func NewRepository(store eventstore.Store, snapshotEvery int64, log *slog.Logger) *Repository {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Repository{store: store, snapshotEvery: snapshotEvery, log: log}
}

func (r *Repository) load(ctx context.Context, streamID string, agg aggregate, useSnapshot bool) error {
	var version int64
	if useSnapshot && r.snapshotEvery > 0 {
		snap, err := r.store.LoadSnapshot(ctx, streamID)
		switch {
		case err == nil:
			if err := json.Unmarshal(snap.State, agg); err != nil {
				return fmt.Errorf("decode snapshot %s: %w", streamID, err)
			}
			version = snap.Version
		case !errors.Is(err, eventstore.ErrNotFound):
			return fmt.Errorf("load snapshot %s: %w", streamID, err)
		}
	}
	events, err := r.store.Load(ctx, streamID, version)
	if err != nil {
		return fmt.Errorf("load stream %s: %w", streamID, err)
	}
	for _, stored := range events {
		ev, err := domain.Decode(stored.Type, stored.Data)
		if err != nil {
			return fmt.Errorf("stream %s v%d: %w", streamID, stored.Version, err)
		}
		if err := agg.Apply(ev); err != nil {
			return fmt.Errorf("stream %s v%d: %w", streamID, stored.Version, err)
		}
		version = stored.Version
	}
	agg.Loaded(version)
	return nil
}

// Account loads an account, returning a not_found domain error if absent.
func (r *Repository) Account(ctx context.Context, id uuid.UUID) (*domain.Account, error) {
	a := &domain.Account{}
	if err := r.load(ctx, domain.AccountStream(id), a, true); err != nil {
		return nil, err
	}
	if !a.Exists() {
		return nil, domain.Errorf(domain.CodeNotFound, "account %s not found", id)
	}
	return a, nil
}

// Entry loads a journal entry.
func (r *Repository) Entry(ctx context.Context, id uuid.UUID) (*domain.Entry, error) {
	e := &domain.Entry{}
	if err := r.load(ctx, domain.EntryStream(id), e, true); err != nil {
		return nil, err
	}
	if !e.Exists() {
		return nil, domain.Errorf(domain.CodeNotFound, "entry %s not found", id)
	}
	return e, nil
}

// Hold loads a hold.
func (r *Repository) Hold(ctx context.Context, id uuid.UUID) (*domain.Hold, error) {
	h := &domain.Hold{}
	if err := r.load(ctx, domain.HoldStream(id), h, true); err != nil {
		return nil, err
	}
	if !h.Exists() {
		return nil, domain.Errorf(domain.CodeNotFound, "hold %s not found", id)
	}
	return h, nil
}

// ReplayAccount rebuilds an account from its full event history, ignoring
// snapshots. Used by consistency checks and tests.
func (r *Repository) ReplayAccount(ctx context.Context, id uuid.UUID) (*domain.Account, error) {
	a := &domain.Account{}
	if err := r.load(ctx, domain.AccountStream(id), a, false); err != nil {
		return nil, err
	}
	return a, nil
}

// Envelope is the integration event published through the outbox.
type Envelope struct {
	EventID    uuid.UUID       `json:"event_id"`
	Type       string          `json:"type"`
	StreamID   string          `json:"stream_id"`
	Version    int64           `json:"version"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
}

type pending struct {
	streamID string
	agg      aggregate
	creates  bool
}

// unitOfWork gathers changes from several aggregates into one atomic append.
type unitOfWork struct {
	items []pending
}

func (u *unitOfWork) track(streamID string, agg aggregate) {
	u.items = append(u.items, pending{streamID: streamID, agg: agg, creates: agg.Version() == 0})
}

func (r *Repository) commit(ctx context.Context, u *unitOfWork, topic string, now time.Time) error {
	appends := make([]eventstore.Append, 0, len(u.items))
	var outbox []eventstore.OutboxMessage
	for _, it := range u.items {
		changes := it.agg.Changes()
		if len(changes) == 0 {
			continue
		}
		a := eventstore.Append{StreamID: it.streamID, ExpectedVersion: it.agg.Version()}
		for i, ch := range changes {
			typ, data, err := domain.Encode(ch)
			if err != nil {
				return err
			}
			id, err := uuid.NewV7()
			if err != nil {
				return fmt.Errorf("event id: %w", err)
			}
			a.Events = append(a.Events, eventstore.NewEvent{ID: id, Type: typ, Data: data})
			if topic == "" {
				continue
			}
			payload, err := json.Marshal(Envelope{
				EventID: id, Type: typ, StreamID: it.streamID,
				Version: it.agg.Version() + int64(i) + 1, OccurredAt: now, Data: data,
			})
			if err != nil {
				return fmt.Errorf("encode envelope: %w", err)
			}
			outbox = append(outbox, eventstore.OutboxMessage{
				ID: id, Topic: topic, Key: it.streamID, Payload: payload,
				Headers: map[string]string{"message-id": id.String(), "event-type": typ},
			})
		}
		appends = append(appends, a)
	}
	if len(appends) == 0 {
		return nil
	}
	if _, err := r.store.Append(ctx, appends, outbox); err != nil {
		var ce *eventstore.ConflictError
		if errors.As(err, &ce) {
			for _, it := range u.items {
				if it.creates && it.streamID == ce.StreamID {
					return domain.Errorf(domain.CodeAlreadyExists, "%s already exists", ce.StreamID)
				}
			}
		}
		return err
	}
	r.snapshot(ctx, u)
	return nil
}

func (r *Repository) snapshot(ctx context.Context, u *unitOfWork) {
	if r.snapshotEvery <= 0 {
		return
	}
	for _, it := range u.items {
		before := it.agg.Version()
		after := before + int64(len(it.agg.Changes()))
		if after/r.snapshotEvery == before/r.snapshotEvery {
			continue
		}
		state, err := json.Marshal(it.agg)
		if err != nil {
			r.log.WarnContext(ctx, "snapshot encode failed", "stream", it.streamID, "err", err)
			continue
		}
		if err := r.store.SaveSnapshot(ctx, eventstore.Snapshot{StreamID: it.streamID, Version: after, State: state}); err != nil {
			r.log.WarnContext(ctx, "snapshot save failed", "stream", it.streamID, "err", err)
		}
	}
}
