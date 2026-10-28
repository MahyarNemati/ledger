package idempotency_test

import (
	"bytes"
	"testing"

	"github.com/MahyarNemati/ledger/internal/idempotency"
	"github.com/MahyarNemati/ledger/internal/idempotency/storetest"
)

func TestMemoryConformance(t *testing.T) {
	storetest.Run(t, func(*testing.T) idempotency.Store { return idempotency.NewMemory() })
}

func TestHashIgnoresWhitespace(t *testing.T) {
	a := idempotency.Hash("POST", "/x", []byte(`{"a": 1, "b": [1, 2]}`))
	b := idempotency.Hash("POST", "/x", []byte("{\n  \"a\":1,\"b\":[1,2]\n}"))
	if !bytes.Equal(a, b) {
		t.Fatal("whitespace changed the hash")
	}
	if bytes.Equal(a, idempotency.Hash("POST", "/y", []byte(`{"a":1,"b":[1,2]}`))) {
		t.Fatal("path must be part of the hash")
	}
}

func TestDeriveIDIsStable(t *testing.T) {
	if idempotency.DeriveID("s", "k") != idempotency.DeriveID("s", "k") {
		t.Fatal("not stable")
	}
	if idempotency.DeriveID("s", "k") == idempotency.DeriveID("t", "k") {
		t.Fatal("scope ignored")
	}
}
