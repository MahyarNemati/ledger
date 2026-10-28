package eventstore_test

import (
	"testing"

	"github.com/MahyarNemati/ledger/internal/eventstore"
	"github.com/MahyarNemati/ledger/internal/eventstore/storetest"
)

func TestMemoryConformance(t *testing.T) {
	storetest.Run(t, func(*testing.T) eventstore.Store { return eventstore.NewMemory() })
}
