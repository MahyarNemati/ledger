package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MahyarNemati/ledger/internal/eventstore"
)

// EventStore is an append-only event log with per-stream optimistic
// concurrency. Stream versions live in the streams table, which acts as the
// lock: a writer either inserts the row (new stream) or advances it from the
// expected version, and loses the race cleanly otherwise.
type EventStore struct {
	pool *pgxpool.Pool
}

func NewEventStore(pool *pgxpool.Pool) *EventStore { return &EventStore{pool: pool} }

var _ eventstore.Store = (*EventStore)(nil)

func (s *EventStore) Load(ctx context.Context, streamID string, after int64) ([]eventstore.Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, stream_id, version, global_position, event_type, data, metadata, recorded_at
		FROM events WHERE stream_id = $1 AND version > $2 ORDER BY version`, streamID, after)
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", streamID, err)
	}
	return collectEvents(rows)
}

// ReadAll only returns events from transactions older than every transaction
// still in flight. Identity values are assigned at insert time, not commit
// time, so without this guard a reader could see position 11 before a slower
// transaction commits position 10 and skip it forever.
func (s *EventStore) ReadAll(ctx context.Context, after int64, limit int) ([]eventstore.Event, error) {
	var lim *int
	if limit > 0 {
		lim = &limit
	}
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, stream_id, version, global_position, event_type, data, metadata, recorded_at
		FROM events
		WHERE global_position > $1 AND tx_id < pg_snapshot_xmin(pg_current_snapshot())
		ORDER BY global_position
		LIMIT $2`, after, lim)
	if err != nil {
		return nil, fmt.Errorf("read all: %w", err)
	}
	return collectEvents(rows)
}

func collectEvents(rows pgx.Rows) ([]eventstore.Event, error) {
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (eventstore.Event, error) {
		var e eventstore.Event
		var meta []byte
		err := r.Scan(&e.ID, &e.StreamID, &e.Version, &e.GlobalPosition, &e.Type, &e.Data, &meta, &e.RecordedAt)
		if meta != nil {
			e.Metadata = json.RawMessage(meta)
		}
		return e, err
	})
}

func (s *EventStore) Append(ctx context.Context, appends []eventstore.Append, outbox []eventstore.OutboxMessage) ([]eventstore.Event, error) {
	if err := eventstore.Validate(appends); err != nil {
		return nil, err
	}
	// A fixed lock order prevents deadlocks between multi-stream writers.
	ordered := append([]eventstore.Append(nil), appends...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].StreamID < ordered[j].StreamID })

	var written []eventstore.Event
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, a := range ordered {
			if err := claimVersion(ctx, tx, a); err != nil {
				return err
			}
		}
		// Insert in caller order so global positions follow causal order.
		batch := &pgx.Batch{}
		for _, a := range appends {
			v := a.ExpectedVersion
			for _, ne := range a.Events {
				v++
				ev := eventstore.Event{ID: ne.ID, StreamID: a.StreamID, Version: v, Type: ne.Type, Data: ne.Data, Metadata: ne.Metadata}
				var meta any
				if len(ne.Metadata) > 0 {
					meta = []byte(ne.Metadata)
				}
				idx := len(written)
				written = append(written, ev)
				batch.Queue(`INSERT INTO events (event_id, stream_id, version, event_type, data, metadata)
					VALUES ($1, $2, $3, $4, $5, $6) RETURNING global_position, recorded_at`,
					ne.ID, a.StreamID, v, ne.Type, []byte(ne.Data), meta,
				).QueryRow(func(row pgx.Row) error {
					return row.Scan(&written[idx].GlobalPosition, &written[idx].RecordedAt)
				})
			}
		}
		for _, m := range outbox {
			headers, err := json.Marshal(m.Headers)
			if err != nil {
				return err
			}
			if m.Headers == nil {
				headers = []byte("{}")
			}
			batch.Queue(`INSERT INTO outbox (id, topic, msg_key, payload, headers) VALUES ($1, $2, $3, $4, $5)`,
				m.ID, m.Topic, m.Key, []byte(m.Payload), headers)
		}
		return tx.SendBatch(ctx, batch).Close()
	})
	if err != nil {
		return nil, err
	}
	return written, nil
}

func claimVersion(ctx context.Context, tx pgx.Tx, a eventstore.Append) error {
	next := a.ExpectedVersion + int64(len(a.Events))
	var sql string
	var args []any
	if a.ExpectedVersion == eventstore.NoStream {
		sql = `INSERT INTO streams (stream_id, version) VALUES ($1, $2) ON CONFLICT (stream_id) DO NOTHING`
		args = []any{a.StreamID, next}
	} else {
		sql = `UPDATE streams SET version = $3, updated_at = now() WHERE stream_id = $1 AND version = $2`
		args = []any{a.StreamID, a.ExpectedVersion, next}
	}
	tag, err := tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("claim %s: %w", a.StreamID, err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var actual int64
	err = tx.QueryRow(ctx, `SELECT version FROM streams WHERE stream_id = $1`, a.StreamID).Scan(&actual)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("read version of %s: %w", a.StreamID, err)
	}
	return &eventstore.ConflictError{StreamID: a.StreamID, Expected: a.ExpectedVersion, Actual: actual}
}

func (s *EventStore) LoadSnapshot(ctx context.Context, streamID string) (eventstore.Snapshot, error) {
	snap := eventstore.Snapshot{StreamID: streamID}
	err := s.pool.QueryRow(ctx, `SELECT version, state FROM snapshots WHERE stream_id = $1`, streamID).Scan(&snap.Version, &snap.State)
	if errors.Is(err, pgx.ErrNoRows) {
		return eventstore.Snapshot{}, eventstore.ErrNotFound
	}
	if err != nil {
		return eventstore.Snapshot{}, fmt.Errorf("load snapshot %s: %w", streamID, err)
	}
	return snap, nil
}

func (s *EventStore) SaveSnapshot(ctx context.Context, snap eventstore.Snapshot) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO snapshots (stream_id, version, state) VALUES ($1, $2, $3)
		ON CONFLICT (stream_id) DO UPDATE SET version = EXCLUDED.version, state = EXCLUDED.state, updated_at = now()
		WHERE snapshots.version < EXCLUDED.version`, snap.StreamID, snap.Version, []byte(snap.State))
	if err != nil {
		return fmt.Errorf("save snapshot %s: %w", snap.StreamID, err)
	}
	return nil
}
