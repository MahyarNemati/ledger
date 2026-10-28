//go:build integration

// Package pgtest provisions an isolated, migrated Postgres schema per test.
// It uses LEDGER_TEST_DATABASE_URL when set and otherwise starts a throwaway
// container with testcontainers, skipping when Docker is unavailable.
package pgtest

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/MahyarNemati/ledger/internal/postgres"
)

var (
	once    sync.Once
	baseDSN string
	baseErr error
)

func dsn(t *testing.T) string {
	t.Helper()
	once.Do(func() {
		if v := os.Getenv("LEDGER_TEST_DATABASE_URL"); v != "" {
			baseDSN = v
			return
		}
		ctx := context.Background()
		testcontainers.SkipIfProviderIsNotHealthy(t)
		c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
			tcpostgres.WithDatabase("ledger"), tcpostgres.WithUsername("ledger"), tcpostgres.WithPassword("ledger"),
			testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)),
		)
		if err != nil {
			baseErr = err
			return
		}
		baseDSN, baseErr = c.ConnectionString(ctx, "sslmode=disable")
	})
	if baseErr != nil {
		t.Fatalf("postgres: %v", baseErr)
	}
	return baseDSN
}

// Pool returns a pool bound to a fresh schema with all migrations applied.
// The schema is dropped when the test finishes.
func Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	base := dsn(t)
	schema := "t_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]

	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close(context.Background())
	})

	cfg, err := pgxpool.ParseConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := postgres.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

// Eventually polls cond until it holds or the timeout elapses. Reads of the
// global log are deliberately held back while older transactions are in
// flight anywhere in the cluster, so convergence checks must poll.
func Eventually(t *testing.T, timeout time.Duration, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := cond()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(fmt.Errorf("condition not met after %s: %w", timeout, err))
		}
		time.Sleep(20 * time.Millisecond)
	}
}
