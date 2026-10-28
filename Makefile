GO ?= go
export CGO_ENABLED ?= 0
COMPOSE ?= docker compose

.PHONY: all build test lint fmt vet cover up down migrate serve relay clean test-race

all: lint test

build:
	$(GO) build -trimpath -ldflags "-s -w" -o bin/ledger ./cmd/ledger

# The race detector requires cgo. This host's macOS SDK cannot link cgo
# (tapi "unknown architecture" errors), so the default test target runs
# without it and `make test-race` is provided for hosts where cgo works.
# CI runs the race detector on Linux.
## test: unit tests, including the property-based invariant suite
test:
	$(GO) test -count=1 ./...

## test-race: same suite with the race detector (needs a working cgo toolchain)
test-race:
	CGO_ENABLED=1 $(GO) test -race -count=1 ./...

## test-integration: also run the Postgres and Kafka suites (needs 'make up')
test-integration:
	LEDGER_TEST_DATABASE_URL=postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable \
	LEDGER_TEST_KAFKA_BROKERS=localhost:9092 \
	CGO_ENABLED=1 $(GO) test -race -count=1 -tags=integration ./...

cover:
	$(GO) test -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1

lint: fmt vet

fmt:
	@unformatted=$$(gofmt -l . | grep -v '^$$' || true); \
	if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi

vet:
	$(GO) vet ./...

## up: Postgres + Kafka + the ledger services
up:
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) down -v

migrate: build
	./bin/ledger migrate

serve: build
	./bin/ledger serve

relay: build
	./bin/ledger relay

clean:
	rm -rf bin coverage.out
