# ledger

[![CI](https://github.com/MahyarNemati/ledger/actions/workflows/ci.yml/badge.svg)](https://github.com/MahyarNemati/ledger/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/MahyarNemati/ledger)](https://goreportcard.com/report/github.com/MahyarNemati/ledger)

A double-entry accounting ledger built on event sourcing: an append-only event
log as the system of record, Postgres projections for balances, idempotency
keys on every mutating endpoint, and the transactional outbox pattern to
publish to Kafka without ever losing or duplicating an event.

The core invariant — **every entry balances, and the sum of all balances is
zero** — is not asserted on hand-picked examples. It is checked against
thousands of randomly generated operation sequences with property-based testing.

```bash
docker compose up -d --build --wait

# Accounts get server-generated UUIDs; amounts are integer minor units
# (2500 = $25.00), because money is never a float.
ALICE=$(curl -s localhost:8080/v1/accounts -H 'Idempotency-Key: k1' \
  -d '{"name":"Alice","type":"asset","currency":"USD"}' | jq -r .id)
BOB=$(curl -s localhost:8080/v1/accounts -H 'Idempotency-Key: k2' \
  -d '{"name":"Bob","type":"liability","currency":"USD"}' | jq -r .id)

curl -s localhost:8080/v1/transfers -H 'Idempotency-Key: t1' \
  -d "{\"debit_account_id\":\"$ALICE\",\"credit_account_id\":\"$BOB\",
       \"amount\":2500,\"currency\":\"USD\",\"description\":\"invoice 1\"}"

curl -s localhost:8080/v1/accounts/$BOB/balance
```

Now replay that `POST /v1/transfers` with the same `Idempotency-Key` as many
times as you like. Bob is paid once, and every replay returns the *original*
response body.

## Why double-entry, and why events

**Double-entry** is not an accounting formality, it is an error-detection
mechanism invented in the 15th century and never improved upon. Every movement
of value is recorded twice, as a debit and an equal credit. If the two sides of
your books ever disagree, you have a bug — and you find out by running a sum
rather than by a customer complaining.

A `balance` column that you `UPDATE` cannot tell you *why* it holds the value it
holds. **Event sourcing** inverts that: the immutable sequence of things that
happened is the truth, and balances are a *derived* read model you can throw
away and rebuild. For money this matters for a specific, practical reason: when
someone asks "why is this account £4.31 short?", you can answer. You replay.

The cost is honest and worth stating: reads need projections, projections lag,
and schema evolution of events is forever. Those trade-offs are in
[Limitations](#limitations).

## Architecture

```
  client
    │  POST /v1/transfers   Idempotency-Key: t1
    ▼
┌─────────────────────────────────────────────────┐
│ httpapi                                         │
│   idempotency middleware ── replay? ──▶ cached  │
│                                         response│
└───────────────────┬─────────────────────────────┘
                    ▼
            ┌───────────────┐
            │    ledger     │  domain invariants:
            │    service    │  balanced entries, holds,
            └───────┬───────┘  currency match, no overdraw
                    │
     ┌──────────ONE TRANSACTION──────────┐
     ▼                                   ▼
┌──────────┐                      ┌──────────┐
│eventstore│  append-only,        │  outbox  │  messages to publish
│          │  optimistic concur.  │          │
└────┬─────┘                      └────┬─────┘
     │                                 │
     │ projector                       │ relay (separate process)
     ▼                                 ▼
┌──────────┐                      ┌──────────┐
│ balances │  read models         │  Kafka   │──▶ consumer ──▶ inbox
│ statement│                      └──────────┘     (dedupe)
└──────────┘
```

| Package | Responsibility |
|---|---|
| `internal/domain` | aggregates and events: account, entry, hold |
| `internal/money` | integer-based amounts and currency; no floats, ever |
| `internal/ledger` | the application service and its invariants |
| `internal/eventstore` | append-only log port + in-memory implementation |
| `internal/idempotency` | idempotency key store port + in-memory |
| `internal/outbox` | outbox port + in-memory |
| `internal/projection` | balance/statement read models and the projector loop |
| `internal/postgres` | the real adapters, plus SQL migrations |
| `internal/kafka` | producer (used by `relay`) and consumer |
| `internal/consumer` | example downstream activity consumer with an inbox |
| `internal/httpapi` | HTTP handlers, DTOs, problem-style errors |

## The three things worth reading the code for

### 1. Money is never a float

`internal/money` stores amounts as scaled integers with an explicit currency.
`0.1 + 0.2 != 0.3` in binary floating point, and in a ledger that discrepancy
does not stay invisible — it accumulates and then someone has to explain it.
Operations on mismatched currencies are a domain error, not an implicit
conversion.

### 2. The outbox, because you cannot be atomic across two systems

The obvious implementation of "save the event and publish it to Kafka" is
broken, and it is broken in a way that testing rarely catches:

```go
tx.Commit()          // ✅ committed
producer.Publish(ev) // 💥 process dies here — event exists, nobody told
```

Publish first instead and you get the opposite bug: consumers act on an event
that was never committed.

There is no atomic commit across Postgres and Kafka. So the event **and** the
intent-to-publish are written in **one Postgres transaction**, and a separate
`relay` process reads the outbox and publishes. If the relay dies, it retries;
if it publishes twice, the consumer's **inbox** table dedupes by message id.

That gives at-least-once delivery with effectively-once processing, which is the
strongest guarantee actually available. Anyone who claims exactly-once delivery
across a network boundary is selling something.

This is also why `relay` is a separate process in the compose file: the API
must never block on Kafka being reachable.

### 3. Idempotency keys that return the *original* response

Every mutating endpoint requires an `Idempotency-Key`. A replay does not just
avoid re-applying the effect — it returns the **same response body** as the
first call, from the idempotency store.

That detail matters more than it looks. A client that retries after a timeout
needs to learn the id of the transfer it created. Returning `409 Conflict`
forces it to guess; returning the original `201` and body lets it carry on.
Keys expire after `LEDGER_IDEMPOTENCY_TTL`.

## Property-based invariant testing

`internal/ledger/invariants_test.go` uses
[rapid](https://pkg.go.dev/pgregory.net/rapid) to generate random sequences of
opens, transfers, holds, captures, voids and reversals, then asserts after every
step that:

- every posted entry has equal debits and credits,
- the sum of all account balances is zero,
- no account is overdrawn beyond its allowance,
- holds never exceed the available balance,
- replaying the event log from zero reproduces the current projections exactly.

The last one is the real test of an event-sourced system: if replay and
incremental projection ever disagree, your read models are silently lying.

When rapid finds a failure it **shrinks** it to a minimal reproducing sequence,
so you get a three-operation counterexample instead of a 400-step log. CI runs
this with `-rapid.checks=2000`.

```bash
go test ./internal/ledger/...                        # quick
go test -rapid.checks=50000 ./internal/ledger/...     # before you trust it
```

## API

Full spec: [`api/openapi.yaml`](api/openapi.yaml).

| Method | Path | Idempotent |
|---|---|:-:|
| `POST` | `/v1/accounts` | ✓ |
| `GET` | `/v1/accounts/{id}` | |
| `POST` | `/v1/accounts/{id}/close` | ✓ |
| `GET` | `/v1/accounts/{id}/balance` | |
| `GET` | `/v1/accounts/{id}/statement` | |
| `POST` | `/v1/entries` | ✓ |
| `GET` | `/v1/entries/{id}` | |
| `POST` | `/v1/entries/{id}/reverse` | ✓ |
| `POST` | `/v1/transfers` | ✓ |
| `POST` | `/v1/holds` | ✓ |
| `GET` | `/v1/holds/{id}` | |
| `POST` | `/v1/holds/{id}/capture` | ✓ |
| `POST` | `/v1/holds/{id}/void` | ✓ |
| `GET` | `/healthz` `/readyz` | |

Entries are **reversed, never deleted.** A ledger is an audit trail; correcting
a mistake means recording the correction. `DELETE` does not appear in this API
by design.

**Holds** (authorise now, capture later) are how card payments actually work:
reserve funds against the available balance, then capture up to the held amount
or void it. Modelling that rather than only immediate transfers is what makes
this a payments ledger instead of a toy.

## Commands

```bash
ledger serve     # HTTP API + balance projector
ledger relay     # publish outbox messages to Kafka
ledger consume   # example activity consumer (inbox dedupe)
ledger migrate   # apply migrations and exit
```

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `LEDGER_STORE` | `memory` | `memory` or `postgres` |
| `LEDGER_DATABASE_URL` | — | required when `LEDGER_STORE=postgres` |
| `LEDGER_AUTO_MIGRATE` | `false` | run migrations at startup |
| `LEDGER_HTTP_ADDR` | `:8080` | API listen address |
| `LEDGER_KAFKA_BROKERS` | — | comma-separated brokers |
| `LEDGER_OUTBOX_TOPIC` | `ledger.events` | topic the relay publishes to |
| `LEDGER_CONSUMER_GROUP` | — | consumer group for `consume` |
| `LEDGER_RELAY_INTERVAL` | — | outbox poll interval |
| `LEDGER_PROJECTION_INTERVAL` | — | projector poll interval |
| `LEDGER_IDEMPOTENCY_TTL` | — | how long keys are remembered |
| `LEDGER_SNAPSHOT_EVERY` | — | events between aggregate snapshots |
| `LEDGER_SHUTDOWN_TIMEOUT` | — | graceful shutdown budget |
| `LEDGER_LOG_LEVEL` / `LEDGER_LOG_FORMAT` | `info` / `json` | logging |

`LEDGER_STORE=memory` runs the whole ledger with no dependencies at all, which
is what makes the unit suite fast.

## Development

```bash
make test               # unit + invariants
make test-race          # the same with the race detector (needs working cgo)
make up                 # Postgres + Kafka + all three processes
make test-integration   # the Postgres and Kafka suites
make lint
```

The race detector requires cgo, so it is a separate target rather than the
default; CI runs it on Linux. Release builds set `CGO_ENABLED=0` for a static
binary.

The in-memory and Postgres adapters are held to **one shared suite**
(`internal/*/storetest`), so the fast fakes the unit tests use are proven to
behave like the real thing. That is the same trick worth copying anywhere you
have a port with two implementations.

Integration tests are behind `-tags=integration` and skip without
`LEDGER_TEST_DATABASE_URL` / `LEDGER_TEST_KAFKA_BROKERS`.

## Limitations

- **Balances are eventually consistent.** `GET /v1/accounts/{id}/balance` is a
  projection, so a transfer followed immediately by a balance read may be
  stale. `GET /v1/accounts/{id}` is strongly consistent by contrast — it is
  rebuilt from events on demand. The API labels which is which rather than
  pretending the distinction does not exist; choosing between them is the
  caller's decision to make.
- **No authentication or multi-tenancy.** Put it behind a gateway.
- **Events have no formal schema registry.** Once an event shape is in the log
  it is immutable forever; a production deployment wants versioned events and
  upcasting.
- **Single-currency entries.** No FX conversion, rates, or multi-currency
  transfers.
- **No partitioned ordering guarantees** beyond per-aggregate; the relay
  publishes in outbox order, not global event order.

## License

MIT — see [LICENSE](LICENSE).
