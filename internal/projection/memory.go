package projection

import (
	"context"
	"sort"
	"sync"

	"github.com/google/uuid"
)

// Memory is an in-process read model. Its transactions hold a lock but do not
// roll back partial writes, which is acceptable for tests and local runs.
type Memory struct {
	mu          sync.Mutex
	balances    map[uuid.UUID]Balance
	statements  map[uuid.UUID][]StatementLine
	checkpoints map[string]int64
}

func NewMemory() *Memory {
	return &Memory{
		balances:    make(map[uuid.UUID]Balance),
		statements:  make(map[uuid.UUID][]StatementLine),
		checkpoints: make(map[string]int64),
	}
}

type memTx struct{ m *Memory }

func (m *Memory) InTx(ctx context.Context, fn func(context.Context, Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return fn(ctx, memTx{m})
}

func (t memTx) GetBalance(_ context.Context, id uuid.UUID) (Balance, error) {
	b, ok := t.m.balances[id]
	if !ok {
		return Balance{}, ErrNotFound
	}
	return b, nil
}

func (t memTx) PutBalance(_ context.Context, b Balance) error {
	t.m.balances[b.AccountID] = b
	return nil
}

func (t memTx) AddStatementLine(_ context.Context, l StatementLine) error {
	t.m.statements[l.AccountID] = append(t.m.statements[l.AccountID], l)
	return nil
}

func (t memTx) Checkpoint(_ context.Context, name string) (int64, error) {
	return t.m.checkpoints[name], nil
}

func (t memTx) SetCheckpoint(_ context.Context, name string, pos int64) error {
	t.m.checkpoints[name] = pos
	return nil
}

func (m *Memory) Balance(_ context.Context, id uuid.UUID) (Balance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.balances[id]
	if !ok {
		return Balance{}, ErrNotFound
	}
	return b, nil
}

func (m *Memory) Statement(_ context.Context, id uuid.UUID, after int64, limit int) ([]StatementLine, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	lines := m.statements[id]
	i := sort.Search(len(lines), func(i int) bool { return lines[i].Position > after })
	out := make([]StatementLine, 0, limit)
	for ; i < len(lines) && len(out) < limit; i++ {
		out = append(out, lines[i])
	}
	return out, nil
}

func (m *Memory) Checkpoint(_ context.Context, name string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.checkpoints[name], nil
}

// Balances returns a copy of every projected balance.
func (m *Memory) Balances() map[uuid.UUID]Balance {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[uuid.UUID]Balance, len(m.balances))
	for k, v := range m.balances {
		out[k] = v
	}
	return out
}
