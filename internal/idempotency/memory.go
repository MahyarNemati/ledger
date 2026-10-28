package idempotency

import (
	"bytes"
	"context"
	"sync"
	"time"
)

type memRecord struct {
	hash      []byte
	rec       *Record
	createdAt time.Time
}

// Memory is an in-process Store.
type Memory struct {
	mu   sync.Mutex
	data map[string]*memRecord
	now  func() time.Time
}

func NewMemory() *Memory {
	return &Memory{data: make(map[string]*memRecord), now: time.Now}
}

func memKey(scope, key string) string { return scope + "\x00" + key }

func (m *Memory) Begin(_ context.Context, scope, key string, hash []byte) (*Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.data[memKey(scope, key)]
	if !ok {
		m.data[memKey(scope, key)] = &memRecord{hash: hash, createdAt: m.now()}
		return nil, nil
	}
	if !bytes.Equal(r.hash, hash) {
		return nil, ErrMismatch
	}
	if r.rec == nil {
		return nil, ErrInProgress
	}
	cp := *r.rec
	return &cp, nil
}

func (m *Memory) Complete(_ context.Context, scope, key string, rec Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.data[memKey(scope, key)]; ok {
		r.rec = &Record{StatusCode: rec.StatusCode, Body: append([]byte(nil), rec.Body...)}
	}
	return nil
}

func (m *Memory) Release(_ context.Context, scope, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.data[memKey(scope, key)]; ok && r.rec == nil {
		delete(m.data, memKey(scope, key))
	}
	return nil
}

func (m *Memory) Purge(_ context.Context, before time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for k, r := range m.data {
		if r.createdAt.Before(before) {
			delete(m.data, k)
			n++
		}
	}
	return n, nil
}
