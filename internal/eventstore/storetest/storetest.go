// Package storetest is a conformance suite that every eventstore.Store
// implementation must pass.
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/MahyarNemati/ledger/internal/eventstore"
)

// Factory returns an empty store for one subtest.
type Factory func(t *testing.T) eventstore.Store

func ev(typ string, n int) eventstore.NewEvent {
	return eventstore.NewEvent{ID: uuid.New(), Type: typ, Data: json.RawMessage(fmt.Sprintf(`{"n":%d}`, n))}
}

func stream() string { return "test-" + uuid.NewString() }

// Run executes the suite.
func Run(t *testing.T, newStore Factory) {
	t.Run("AppendAndLoad", func(t *testing.T) {
		s, ctx, id := newStore(t), context.Background(), stream()
		written, err := s.Append(ctx, []eventstore.Append{{StreamID: id, ExpectedVersion: 0, Events: []eventstore.NewEvent{ev("A", 1), ev("B", 2)}}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(written) != 2 || written[0].Version != 1 || written[1].Version != 2 {
			t.Fatalf("unexpected versions: %+v", written)
		}
		if written[1].GlobalPosition <= written[0].GlobalPosition {
			t.Fatalf("global positions not increasing: %d, %d", written[0].GlobalPosition, written[1].GlobalPosition)
		}
		got, err := s.Load(ctx, id, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0].Type != "A" || got[1].Type != "B" {
			t.Fatalf("load returned %+v", got)
		}
		var payload struct{ N int }
		if err := json.Unmarshal(got[1].Data, &payload); err != nil || payload.N != 2 {
			t.Fatalf("payload not preserved: %s", got[1].Data)
		}
		tail, err := s.Load(ctx, id, 1)
		if err != nil || len(tail) != 1 || tail[0].Version != 2 {
			t.Fatalf("load after version 1: %+v, %v", tail, err)
		}
		empty, err := s.Load(ctx, stream(), 0)
		if err != nil || len(empty) != 0 {
			t.Fatalf("missing stream: %+v, %v", empty, err)
		}
	})

	t.Run("VersionConflict", func(t *testing.T) {
		s, ctx, id := newStore(t), context.Background(), stream()
		if _, err := s.Append(ctx, []eventstore.Append{{StreamID: id, Events: []eventstore.NewEvent{ev("A", 1)}}}, nil); err != nil {
			t.Fatal(err)
		}
		_, err := s.Append(ctx, []eventstore.Append{{StreamID: id, ExpectedVersion: 0, Events: []eventstore.NewEvent{ev("A", 2)}}}, nil)
		if !errors.Is(err, eventstore.ErrConcurrency) {
			t.Fatalf("want concurrency error, got %v", err)
		}
		var ce *eventstore.ConflictError
		if !errors.As(err, &ce) || ce.StreamID != id || ce.Actual != 1 {
			t.Fatalf("conflict details wrong: %+v", ce)
		}
		if _, err := s.Append(ctx, []eventstore.Append{{StreamID: id, ExpectedVersion: 5, Events: []eventstore.NewEvent{ev("A", 2)}}}, nil); !errors.Is(err, eventstore.ErrConcurrency) {
			t.Fatalf("ahead-of-stream append: want conflict, got %v", err)
		}
	})

	t.Run("MultiStreamAppendIsAtomic", func(t *testing.T) {
		s, ctx := newStore(t), context.Background()
		a, b := stream(), stream()
		if _, err := s.Append(ctx, []eventstore.Append{{StreamID: b, Events: []eventstore.NewEvent{ev("B", 1)}}}, nil); err != nil {
			t.Fatal(err)
		}
		_, err := s.Append(ctx, []eventstore.Append{
			{StreamID: a, ExpectedVersion: 0, Events: []eventstore.NewEvent{ev("A", 1)}},
			{StreamID: b, ExpectedVersion: 0, Events: []eventstore.NewEvent{ev("B", 2)}},
		}, []eventstore.OutboxMessage{{ID: uuid.New(), Topic: "t", Key: a, Payload: json.RawMessage(`{}`)}})
		if !errors.Is(err, eventstore.ErrConcurrency) {
			t.Fatalf("want conflict, got %v", err)
		}
		if got, _ := s.Load(ctx, a, 0); len(got) != 0 {
			t.Fatalf("stream %s was partially written", a)
		}
	})

	t.Run("RejectsInvalidAppends", func(t *testing.T) {
		s, ctx, id := newStore(t), context.Background(), stream()
		bad := [][]eventstore.Append{
			nil,
			{{StreamID: "", Events: []eventstore.NewEvent{ev("A", 1)}}},
			{{StreamID: id}},
			{{StreamID: id, Events: []eventstore.NewEvent{ev("A", 1)}}, {StreamID: id, Events: []eventstore.NewEvent{ev("A", 2)}}},
		}
		for i, appends := range bad {
			if _, err := s.Append(ctx, appends, nil); err == nil {
				t.Errorf("case %d: expected error", i)
			}
		}
	})

	t.Run("ReadAllIsGloballyOrdered", func(t *testing.T) {
		s, ctx := newStore(t), context.Background()
		start := head(t, s)
		ids := []string{stream(), stream(), stream()}
		for i := 0; i < 9; i++ {
			id := ids[i%3]
			cur, _ := s.Load(ctx, id, 0)
			if _, err := s.Append(ctx, []eventstore.Append{{StreamID: id, ExpectedVersion: int64(len(cur)), Events: []eventstore.NewEvent{ev("X", i)}}}, nil); err != nil {
				t.Fatal(err)
			}
		}
		// Some stores hold back the tail of the log while unrelated
		// transactions are in flight, so poll briefly for the full set.
		var all []eventstore.Event
		after := start
		deadline := time.Now().Add(5 * time.Second)
		for len(all) < 9 && time.Now().Before(deadline) {
			page, err := s.ReadAll(ctx, after, 4)
			if err != nil {
				t.Fatal(err)
			}
			if len(page) == 0 {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			all = append(all, page...)
			after = page[len(page)-1].GlobalPosition
		}
		if len(all) != 9 {
			t.Fatalf("read %d events, want 9", len(all))
		}
		for i := range all {
			var p struct{ N int }
			_ = json.Unmarshal(all[i].Data, &p)
			if p.N != i {
				t.Fatalf("event %d out of order (n=%d)", i, p.N)
			}
		}
	})

	t.Run("Snapshots", func(t *testing.T) {
		s, ctx, id := newStore(t), context.Background(), stream()
		if _, err := s.LoadSnapshot(ctx, id); !errors.Is(err, eventstore.ErrNotFound) {
			t.Fatalf("want not found, got %v", err)
		}
		must(t, s.SaveSnapshot(ctx, eventstore.Snapshot{StreamID: id, Version: 5, State: json.RawMessage(`{"v":5}`)}))
		must(t, s.SaveSnapshot(ctx, eventstore.Snapshot{StreamID: id, Version: 3, State: json.RawMessage(`{"v":3}`)}))
		snap, err := s.LoadSnapshot(ctx, id)
		if err != nil || snap.Version != 5 {
			t.Fatalf("older snapshot must not replace newer: %+v, %v", snap, err)
		}
		must(t, s.SaveSnapshot(ctx, eventstore.Snapshot{StreamID: id, Version: 8, State: json.RawMessage(`{"v":8}`)}))
		if snap, _ := s.LoadSnapshot(ctx, id); snap.Version != 8 {
			t.Fatalf("snapshot not advanced: %+v", snap)
		}
	})

	t.Run("ConcurrentWritersExactlyOneWins", func(t *testing.T) {
		s, ctx, id := newStore(t), context.Background(), stream()
		const writers = 16
		var wins atomic.Int32
		var wg sync.WaitGroup
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, err := s.Append(ctx, []eventstore.Append{{StreamID: id, Events: []eventstore.NewEvent{ev("A", i)}}}, nil)
				switch {
				case err == nil:
					wins.Add(1)
				case !errors.Is(err, eventstore.ErrConcurrency):
					t.Errorf("writer %d: %v", i, err)
				}
			}(i)
		}
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("%d writers won, want exactly 1", wins.Load())
		}
	})
}

func head(t *testing.T, s eventstore.Store) int64 {
	t.Helper()
	var pos int64
	for {
		page, err := s.ReadAll(context.Background(), pos, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			return pos
		}
		pos = page[len(page)-1].GlobalPosition
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
