// Package config loads process configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"
)

// Config is the validated process configuration.
type Config struct {
	HTTPAddr           string
	Store              string
	DatabaseURL        string
	AutoMigrate        bool
	SnapshotEvery      int64
	OutboxTopic        string
	KafkaBrokers       []string
	ConsumerGroup      string
	LogLevel           slog.Level
	LogFormat          string
	IdempotencyTTL     time.Duration
	ProjectionInterval time.Duration
	RelayInterval      time.Duration
	ShutdownTimeout    time.Duration
}

// Load reads configuration through getenv (os.Getenv in production) and
// reports every invalid variable at once.
func Load(getenv func(string) string) (Config, error) {
	p := parser{getenv: getenv}
	c := Config{
		HTTPAddr:           p.str("LEDGER_HTTP_ADDR", ":8080"),
		Store:              p.str("LEDGER_STORE", "postgres"),
		DatabaseURL:        p.str("LEDGER_DATABASE_URL", ""),
		AutoMigrate:        p.boolean("LEDGER_AUTO_MIGRATE", true),
		SnapshotEvery:      p.integer("LEDGER_SNAPSHOT_EVERY", 50, 0, 1_000_000),
		OutboxTopic:        p.str("LEDGER_OUTBOX_TOPIC", "ledger.events"),
		KafkaBrokers:       p.list("LEDGER_KAFKA_BROKERS"),
		ConsumerGroup:      p.str("LEDGER_CONSUMER_GROUP", "ledger-activity"),
		LogFormat:          p.str("LEDGER_LOG_FORMAT", "json"),
		IdempotencyTTL:     p.duration("LEDGER_IDEMPOTENCY_TTL", 24*time.Hour),
		ProjectionInterval: p.duration("LEDGER_PROJECTION_INTERVAL", 200*time.Millisecond),
		RelayInterval:      p.duration("LEDGER_RELAY_INTERVAL", 250*time.Millisecond),
		ShutdownTimeout:    p.duration("LEDGER_SHUTDOWN_TIMEOUT", 15*time.Second),
	}
	if err := c.LogLevel.UnmarshalText([]byte(p.str("LEDGER_LOG_LEVEL", "info"))); err != nil {
		p.fail("LEDGER_LOG_LEVEL", "must be debug, info, warn or error")
	}
	if _, _, err := net.SplitHostPort(c.HTTPAddr); err != nil {
		p.fail("LEDGER_HTTP_ADDR", "must be host:port")
	}
	switch c.Store {
	case "postgres":
		if c.DatabaseURL == "" {
			p.fail("LEDGER_DATABASE_URL", "is required when LEDGER_STORE=postgres")
		}
	case "memory":
	default:
		p.fail("LEDGER_STORE", "must be postgres or memory")
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		p.fail("LEDGER_LOG_FORMAT", "must be json or text")
	}
	if c.OutboxTopic == "" {
		p.fail("LEDGER_OUTBOX_TOPIC", "must not be empty")
	}
	return c, errors.Join(p.errs...)
}

// RequireKafka validates settings needed by the relay and consumer.
func (c Config) RequireKafka() error {
	if len(c.KafkaBrokers) == 0 {
		return errors.New("LEDGER_KAFKA_BROKERS is required")
	}
	if c.Store != "postgres" {
		return errors.New("the outbox relay and consumer need LEDGER_STORE=postgres")
	}
	return nil
}

type parser struct {
	getenv func(string) string
	errs   []error
}

func (p *parser) fail(key, msg string) { p.errs = append(p.errs, fmt.Errorf("%s %s", key, msg)) }

func (p *parser) str(key, def string) string {
	if v := strings.TrimSpace(p.getenv(key)); v != "" {
		return v
	}
	return def
}

func (p *parser) list(key string) []string {
	var out []string
	for _, s := range strings.Split(p.getenv(key), ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func (p *parser) boolean(key string, def bool) bool {
	raw := p.str(key, "")
	if raw == "" {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		p.fail(key, "must be a boolean")
	}
	return v
}

func (p *parser) integer(key string, def, lo, hi int64) int64 {
	raw := p.str(key, "")
	if raw == "" {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < lo || v > hi {
		p.fail(key, fmt.Sprintf("must be an integer between %d and %d", lo, hi))
	}
	return v
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	raw := p.str(key, "")
	if raw == "" {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil || v <= 0 {
		p.fail(key, "must be a positive duration such as 500ms or 24h")
	}
	return v
}
