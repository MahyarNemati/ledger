// Command ledger runs the ledger API, the outbox relay, the example consumer
// or database migrations.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/MahyarNemati/ledger/internal/config"
	"github.com/MahyarNemati/ledger/internal/consumer"
	"github.com/MahyarNemati/ledger/internal/eventstore"
	"github.com/MahyarNemati/ledger/internal/httpapi"
	"github.com/MahyarNemati/ledger/internal/idempotency"
	"github.com/MahyarNemati/ledger/internal/kafka"
	"github.com/MahyarNemati/ledger/internal/ledger"
	"github.com/MahyarNemati/ledger/internal/outbox"
	"github.com/MahyarNemati/ledger/internal/postgres"
	"github.com/MahyarNemati/ledger/internal/projection"
)

const usage = `usage: ledger <command>

commands:
  serve     run the HTTP API and the balance projector
  relay     publish outbox messages to Kafka
  consume   run the example activity consumer
  migrate   apply database migrations and exit
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration:\n%v\n", err)
		os.Exit(2)
	}
	log := newLogger(cfg)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var run func(context.Context, config.Config, *slog.Logger) error
	switch os.Args[1] {
	case "serve":
		run = serve
	case "relay":
		run = relay
	case "consume":
		run = consume
	case "migrate":
		run = migrate
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err := run(ctx, cfg, log); err != nil {
		log.Error("exiting", "command", os.Args[1], "err", err)
		os.Exit(1)
	}
}

func newLogger(cfg config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	if cfg.LogFormat == "text" {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

func openPostgres(ctx context.Context, cfg config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	pool, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	if cfg.AutoMigrate {
		applied, err := postgres.Migrate(ctx, pool)
		if err != nil {
			pool.Close()
			return nil, err
		}
		if len(applied) > 0 {
			log.Info("migrations applied", "names", applied)
		}
	}
	return pool, nil
}

type projectionStore interface {
	projection.Backend
	projection.Reader
}

func serve(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	var (
		store eventstore.Store
		proj  projectionStore
		idem  idempotency.Store
		ready func(context.Context) error
	)
	switch cfg.Store {
	case "postgres":
		pool, err := openPostgres(ctx, cfg, log)
		if err != nil {
			return err
		}
		defer pool.Close()
		store, proj, idem = postgres.NewEventStore(pool), postgres.NewProjections(pool), postgres.NewIdempotencyStore(pool)
		ready = pool.Ping
	default:
		log.Warn("using in-memory storage; data is lost on exit")
		store, proj, idem = eventstore.NewMemory(), projection.NewMemory(), idempotency.NewMemory()
	}

	svc := ledger.NewService(store, ledger.Options{SnapshotEvery: cfg.SnapshotEvery, OutboxTopic: cfg.OutboxTopic, Logger: log})
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.New(httpapi.Deps{Service: svc, Projections: proj, Idempotency: idem, Logger: log, Ready: ready}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		runner := &projection.Runner{Name: "balances", Store: store, Backend: proj, Interval: cfg.ProjectionInterval, Log: log}
		return runner.Run(ctx)
	})
	g.Go(func() error {
		t := time.NewTicker(time.Hour)
		defer t.Stop()
		for {
			if n, err := idem.Purge(ctx, time.Now().Add(-cfg.IdempotencyTTL)); err != nil && ctx.Err() == nil {
				log.Warn("idempotency purge failed", "err", err)
			} else if n > 0 {
				log.Info("idempotency keys purged", "count", n)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
			}
		}
	})
	g.Go(func() error {
		log.Info("http listening", "addr", cfg.HTTPAddr, "store", cfg.Store)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		log.Info("shutting down")
		return srv.Shutdown(shutdownCtx)
	})
	return g.Wait()
}

func relay(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	if err := cfg.RequireKafka(); err != nil {
		return err
	}
	pool, err := openPostgres(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer pool.Close()
	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer producer.Close()
	log.Info("outbox relay started", "brokers", cfg.KafkaBrokers)
	r := &outbox.Relay{Source: postgres.NewOutboxSource(pool), Publisher: producer, Interval: cfg.RelayInterval, Log: log}
	return r.Run(ctx)
}

func consume(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	if err := cfg.RequireKafka(); err != nil {
		return err
	}
	pool, err := openPostgres(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer pool.Close()
	h := &consumer.ActivityHandler{Name: cfg.ConsumerGroup, Store: postgres.NewActivityStore(pool)}
	c, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup, []string{cfg.OutboxTopic}, h.Handle, log)
	if err != nil {
		return err
	}
	log.Info("consumer started", "group", cfg.ConsumerGroup, "topic", cfg.OutboxTopic)
	return c.Run(ctx)
}

func migrate(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	if cfg.Store != "postgres" {
		return errors.New("migrate needs LEDGER_STORE=postgres")
	}
	pool, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	applied, err := postgres.Migrate(ctx, pool)
	if err != nil {
		return err
	}
	log.Info("migrations complete", "applied", applied)
	return nil
}
