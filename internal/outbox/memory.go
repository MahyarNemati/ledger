package outbox

import (
	"context"
	"sync"
)

// Memory is an in-process Source for tests.
type Memory struct {
	mu        sync.Mutex
	pending   []Message
	published []Message
	seq       int64
}

// Add enqueues messages.
func (m *Memory) Add(msgs ...Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, msg := range msgs {
		m.seq++
		msg.Seq = m.seq
		m.pending = append(m.pending, msg)
	}
}

func (m *Memory) Process(ctx context.Context, limit int, fn func(context.Context, []Message) error) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := min(limit, len(m.pending))
	if n == 0 {
		return 0, nil
	}
	batch := append([]Message(nil), m.pending[:n]...)
	if err := fn(ctx, batch); err != nil {
		return 0, err
	}
	m.published = append(m.published, batch...)
	m.pending = m.pending[n:]
	return n, nil
}

// Published returns every message marked published, in order.
func (m *Memory) Published() []Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Message(nil), m.published...)
}

// Pending returns the number of unpublished messages.
func (m *Memory) Pending() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pending)
}
