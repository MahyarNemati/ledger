// Package idempotency stores the outcome of mutating API requests keyed by a
// client-supplied Idempotency-Key so that retries return the original
// response instead of executing twice.
package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrMismatch means the key was reused with a different request.
	ErrMismatch = errors.New("idempotency key reused with a different request")
	// ErrInProgress means another request with the same key has not finished.
	ErrInProgress = errors.New("a request with this idempotency key is in progress")
)

// Record is a stored request outcome.
type Record struct {
	StatusCode int
	Body       []byte
}

// Store persists idempotency records.
type Store interface {
	// Begin claims the key. It returns (nil, nil) when the caller should
	// execute the request, the stored Record when the request already
	// completed, or ErrMismatch / ErrInProgress.
	Begin(ctx context.Context, scope, key string, hash []byte) (*Record, error)
	// Complete stores the response for a claimed key.
	Complete(ctx context.Context, scope, key string, rec Record) error
	// Release drops a claim so that the request can be retried, used when
	// the request failed in a way that should not be replayed.
	Release(ctx context.Context, scope, key string) error
	// Purge deletes records created before the cutoff.
	Purge(ctx context.Context, before time.Time) (int64, error)
}

// Hash fingerprints a request. JSON bodies are compacted first so that
// insignificant whitespace does not count as a different request.
func Hash(method, path string, body []byte) []byte {
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{0})
	h.Write([]byte(path))
	h.Write([]byte{0})
	var buf bytes.Buffer
	if json.Valid(body) && json.Compact(&buf, body) == nil {
		h.Write(buf.Bytes())
	} else {
		h.Write(body)
	}
	return h.Sum(nil)
}

var namespace = uuid.MustParse("6f1d0c7e-2b8a-4f43-9a7e-3c2d6b5a1e90")

// DeriveID maps a key to a stable aggregate ID. Using it for the resource a
// request creates means a retry that slips past the record (for example after
// a crash between commit and Complete) collides on the event stream instead
// of moving money twice.
func DeriveID(scope, key string) uuid.UUID {
	return uuid.NewSHA1(namespace, []byte(scope+"\x00"+key))
}
