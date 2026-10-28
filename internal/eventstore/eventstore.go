// Package eventstore defines the append-only event log contract shared by the
// in-memory and Postgres implementations.
package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrConcurrency is returned when a stream's current version does not match
// the version the writer expected.
var ErrConcurrency = errors.New("eventstore: stream version conflict")

// ErrNotFound is returned when no snapshot exists for a stream.
var ErrNotFound = errors.New("eventstore: not found")

// NoStream is the expected version for a stream that must not exist yet.
const NoStream int64 = 0

// Event is a persisted, immutable fact.
type Event struct {
	ID             uuid.UUID
	StreamID       string
	Version        int64
	GlobalPosition int64
	Type           string
	Data           json.RawMessage
	Metadata       json.RawMessage
	RecordedAt     time.Time
}

// NewEvent is an event that has not been appended yet.
type NewEvent struct {
	ID       uuid.UUID
	Type     string
	Data     json.RawMessage
	Metadata json.RawMessage
}

// Append targets one stream with optimistic concurrency.
type Append struct {
	StreamID        string
	ExpectedVersion int64
	Events          []NewEvent
}

// OutboxMessage is written in the same transaction as the events it describes.
type OutboxMessage struct {
	ID      uuid.UUID
	Topic   string
	Key     string
	Payload json.RawMessage
	Headers map[string]string
}

// Snapshot captures aggregate state at a stream version.
type Snapshot struct {
	StreamID string
	Version  int64
	State    json.RawMessage
}

// ConflictError carries the stream that failed its version check.
type ConflictError struct {
	StreamID string
	Expected int64
	Actual   int64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("eventstore: stream %s at version %d, expected %d", e.StreamID, e.Actual, e.Expected)
}

func (e *ConflictError) Unwrap() error { return ErrConcurrency }

// Store is the persistence port for event-sourced aggregates.
type Store interface {
	// Load returns events of a stream with Version > afterVersion, in order.
	Load(ctx context.Context, streamID string, afterVersion int64) ([]Event, error)
	// Append atomically appends to every stream and enqueues outbox messages.
	// Either all streams pass their version check and everything is written,
	// or nothing is.
	Append(ctx context.Context, appends []Append, outbox []OutboxMessage) ([]Event, error)
	// ReadAll returns events in global order with GlobalPosition > after.
	ReadAll(ctx context.Context, after int64, limit int) ([]Event, error)
	LoadSnapshot(ctx context.Context, streamID string) (Snapshot, error)
	SaveSnapshot(ctx context.Context, s Snapshot) error
}

// Validate checks structural correctness of an append batch.
func Validate(appends []Append) error {
	if len(appends) == 0 {
		return errors.New("eventstore: empty append")
	}
	seen := make(map[string]struct{}, len(appends))
	for _, a := range appends {
		if a.StreamID == "" {
			return errors.New("eventstore: empty stream id")
		}
		if a.ExpectedVersion < 0 {
			return fmt.Errorf("eventstore: negative expected version for %s", a.StreamID)
		}
		if len(a.Events) == 0 {
			return fmt.Errorf("eventstore: no events for %s", a.StreamID)
		}
		if _, dup := seen[a.StreamID]; dup {
			return fmt.Errorf("eventstore: stream %s appears twice in one append", a.StreamID)
		}
		seen[a.StreamID] = struct{}{}
	}
	return nil
}
