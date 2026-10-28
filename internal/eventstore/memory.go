package eventstore

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Memory is a goroutine-safe in-process Store used by tests and local runs.
type Memory struct {
	mu        sync.RWMutex
	streams   map[string][]Event
	all       []Event
	snapshots map[string]Snapshot
	outbox    []OutboxMessage
	now       func() time.Time
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{
		streams:   make(map[string][]Event),
		snapshots: make(map[string]Snapshot),
		now:       time.Now,
	}
}

func (m *Memory) Load(_ context.Context, streamID string, after int64) ([]Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	evs := m.streams[streamID]
	if after >= int64(len(evs)) {
		return nil, nil
	}
	if after < 0 {
		after = 0
	}
	out := make([]Event, len(evs)-int(after))
	copy(out, evs[after:])
	return out, nil
}

func (m *Memory) Append(_ context.Context, appends []Append, outbox []OutboxMessage) ([]Event, error) {
	if err := Validate(appends); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, a := range appends {
		if actual := int64(len(m.streams[a.StreamID])); actual != a.ExpectedVersion {
			return nil, &ConflictError{StreamID: a.StreamID, Expected: a.ExpectedVersion, Actual: actual}
		}
	}
	now := m.now().UTC()
	var written []Event
	for _, a := range appends {
		version := a.ExpectedVersion
		for _, ne := range a.Events {
			version++
			id := ne.ID
			if id == uuid.Nil {
				id = uuid.New()
			}
			ev := Event{
				ID:             id,
				StreamID:       a.StreamID,
				Version:        version,
				GlobalPosition: int64(len(m.all)) + 1,
				Type:           ne.Type,
				Data:           ne.Data,
				Metadata:       ne.Metadata,
				RecordedAt:     now,
			}
			m.streams[a.StreamID] = append(m.streams[a.StreamID], ev)
			m.all = append(m.all, ev)
			written = append(written, ev)
		}
	}
	m.outbox = append(m.outbox, outbox...)
	return written, nil
}

func (m *Memory) ReadAll(_ context.Context, after int64, limit int) ([]Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if after < 0 {
		after = 0
	}
	if after >= int64(len(m.all)) {
		return nil, nil
	}
	end := len(m.all)
	if limit > 0 && int(after)+limit < end {
		end = int(after) + limit
	}
	out := make([]Event, end-int(after))
	copy(out, m.all[after:end])
	return out, nil
}

func (m *Memory) LoadSnapshot(_ context.Context, streamID string) (Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.snapshots[streamID]
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	return s, nil
}

func (m *Memory) SaveSnapshot(_ context.Context, s Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.snapshots[s.StreamID]; ok && cur.Version >= s.Version {
		return nil
	}
	m.snapshots[s.StreamID] = s
	return nil
}

// Outbox returns a copy of every enqueued outbox message.
func (m *Memory) Outbox() []OutboxMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]OutboxMessage, len(m.outbox))
	copy(out, m.outbox)
	return out
}
