GO ?= go
export CGO_ENABLED ?= 0
COMPOSE ?= docker compose

.PHONY: all build test lint fmt vet cover up down migrate serve relay clean

all: lint test

build:
	$(GO) build -trimpath -ldflags "-s -w" -o bin/ledger ./cmd/ledger

## test: unit tests, including the property-based invariant suite
test:
	$(GO) test -race -count=1 ./...

## test-integration: also run the Postgres and Kafka suites (needs 'make up')
test-integration:
	LEDGER_TEST_DATABASE_URL=postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable \
	LEDGER_TEST_KAFKA_BROKERS=localhost:9092 \
	$(GO) test -race -count=1 -tags=integration ./...

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
