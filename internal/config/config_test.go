package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestDefaults(t *testing.T) {
	c, err := Load(env(map[string]string{"LEDGER_DATABASE_URL": "postgres://x"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || c.SnapshotEvery != 50 || c.LogLevel != slog.LevelInfo || c.IdempotencyTTL != 24*time.Hour || !c.AutoMigrate {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	if err := c.RequireKafka(); err == nil {
		t.Fatal("kafka should be required for relay")
	}
}

func TestOverrides(t *testing.T) {
	c, err := Load(env(map[string]string{
		"LEDGER_STORE": "memory", "LEDGER_HTTP_ADDR": "127.0.0.1:9000", "LEDGER_LOG_LEVEL": "debug",
		"LEDGER_KAFKA_BROKERS": "a:9092, b:9092,", "LEDGER_SNAPSHOT_EVERY": "0", "LEDGER_AUTO_MIGRATE": "false",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.KafkaBrokers) != 2 || c.KafkaBrokers[1] != "b:9092" || c.SnapshotEvery != 0 || c.AutoMigrate || c.LogLevel != slog.LevelDebug {
		t.Fatalf("overrides not applied: %+v", c)
	}
}

func TestReportsEveryProblem(t *testing.T) {
	_, err := Load(env(map[string]string{
		"LEDGER_HTTP_ADDR": "nope", "LEDGER_SNAPSHOT_EVERY": "-1", "LEDGER_IDEMPOTENCY_TTL": "soon",
		"LEDGER_LOG_LEVEL": "loud", "LEDGER_STORE": "redis",
	}))
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, key := range []string{"LEDGER_HTTP_ADDR", "LEDGER_SNAPSHOT_EVERY", "LEDGER_IDEMPOTENCY_TTL", "LEDGER_LOG_LEVEL", "LEDGER_STORE"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not mention %s: %v", key, err)
		}
	}
}

func TestPostgresNeedsURL(t *testing.T) {
	if _, err := Load(env(nil)); err == nil || !strings.Contains(err.Error(), "LEDGER_DATABASE_URL") {
		t.Fatalf("got %v", err)
	}
}
