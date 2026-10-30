COMPOSE  := docker compose -f deploy/docker-compose.yml
PSQL     := $(COMPOSE) exec -T postgres psql -U khan -d khan
TEST_PG  := postgres://khan:khan@localhost:5432/khan_test?sslmode=disable
TEST_NATS := nats://localhost:4222

.PHONY: up down reset logs lint test test-int bench fuzz smoke crash-demo load verify psql build

up:
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f api engine settlement

build:
	go build ./...

lint:
	test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)
	go vet ./...

test:
	go test -race -count=1 ./...

test-int:
	$(PSQL) -tAc "SELECT 1 FROM pg_database WHERE datname = 'khan_test'" | grep -q 1 || $(PSQL) -c "CREATE DATABASE khan_test"
	TEST_DATABASE_URL="$(TEST_PG)" TEST_NATS_URL="$(TEST_NATS)" go test -race -count=1 ./...

bench:
	go test -run=NONE -bench=. -benchmem ./internal/book

fuzz:
	go test -run=NONE -fuzz=FuzzBook -fuzztime=30s ./internal/book
	go test -run=NONE -fuzz=FuzzRestore -fuzztime=30s ./internal/book

smoke:
	PSQL="$(PSQL) -tA" ./scripts/smoke.sh

load:
	go run ./cmd/loadgen -orders 5000 -c 16

crash-demo:
	PSQL="$(PSQL)" ./scripts/crash-demo.sh

verify:
	$(PSQL) -v ON_ERROR_STOP=1 < scripts/verify.sql

psql:
	$(COMPOSE) exec postgres psql -U khan -d khan
