.PHONY: help all up down migrate build lint test check clean api worker sfu web web-check e2e load capacity

# Nothing here gains from parallelism, and `all` depends on migrate running after up —
# which under -j, or a -j someone has in MAKEFLAGS, would be goose against a Postgres that
# is not listening yet.
.NOTPARALLEL:

# Loaded so make targets see the same values the binaries do.
ifneq (,$(wildcard .env))
include .env
export
endif

help: ## Show available targets
	@grep -hE '^[a-z0-9-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-10s %s\n", $$1, $$2}'

# Everything, in one terminal. Depends on up and migrate so a clean clone needs one
# command, and both are cheap to repeat: compose returns immediately when the containers
# are already healthy, and goose does nothing when the schema is current.
#
# `kill 0` signals the process group rather than the three recorded pids, because `go run`
# compiles and then execs a separate child: killing the go process leaves that child
# holding :8080, and the next `make all` fails on an address already in use. Which is a
# confusing way to learn that the last one never really stopped.
#
# Forwarding stays inside api here — one machine does not need cmd/sfu, and a call is
# joinable from the only node there is. `make sfu` and SFU_URL are for the split.
# Ctrl-C has to take all three down, and every shorter way of writing this leaks something.
# Three things the handler is doing, each for a reason found by leaving it out:
#
#   kill 0 — the whole process group, not the recorded pids. `go run` compiles and execs a
#   separate child, and `npm run dev` reaches node through two more; signalling the job
#   leader leaves the real server holding :8080 or :5173, so the next `make all` fails on an
#   address in use. That is a confusing way to learn the last one never stopped.
#
#   Disarming first — kill 0 signals this shell too, which would re-enter the handler and
#   kill the group again until the stack ran out. The first version dumped core on Ctrl-C.
#
#   No `set -m` — job control would put this shell in its own process group, and the
#   terminal delivers Ctrl-C to the foreground group only. With it, SIGINT reached make,
#   make died, and all three servers stayed up. Converting the signal here is also what
#   gets them to exit at all: a non-interactive shell's background jobs inherit SIGINT
#   ignored, so Ctrl-C alone never stops them, while the TERM this sends is handled.
all: up migrate ## Start dependencies, then api, worker and the browser client together
	@trap 'trap - EXIT INT TERM; kill 0' EXIT INT TERM; \
	go run ./cmd/api & \
	go run ./cmd/worker & \
	$(MAKE) --no-print-directory web & \
	wait

up: ## Start dependencies and wait until they are healthy
	docker compose up -d --wait

down: ## Stop dependencies, keeping volumes
	docker compose down

migrate: ## Apply database migrations
	go run ./cmd/migrate

build: ## Build all binaries into bin/
	go build -o bin/ ./cmd/...

# The architecture test is part of lint, not just test: a boundary violation is
# the kind of mistake that should stop a change from landing, and lint is what
# people run before pushing. golangci-lint is optional so a clean clone can lint
# with nothing but the Go toolchain installed.
lint: ## Vet, check architectural boundaries, and run golangci-lint if present
	go vet ./...
	go test ./internal/arch/...
	@command -v golangci-lint >/dev/null 2>&1 \
		&& golangci-lint run \
		|| echo "golangci-lint not installed, skipping (see .golangci.yml)"

test: ## Run all tests
	go test ./...

# These exist because nothing in the Go code reads .env — the include above is the only
# thing that does. `go run ./cmd/api` in a bare shell fails on DATABASE_URL, so running
# it through make is the difference between a working start and a confusing one.
api: ## Run the HTTP and WebSocket node on :8080
	go run ./cmd/api

worker: ## Run the outbox relay, projections and media processing
	go run ./cmd/worker

sfu: ## Run call media forwarding as its own process (needs SFU_URL set for api)
	go run ./cmd/sfu

web: ## Run the browser client against a local api on :8080
	cd web && npm install --silent && npm run dev

web-check: ## Typecheck the browser client and run its unit tests
	cd web && npm install --silent && npm run build && npm test

# The media load script. Points at a local stub by default, so it is runnable with
# nothing deployed; -url points it at a real server, -node at cmd/sfu. See internal/harness.
load: ## Drive a media server with simulated participants (PEERS=20 FOR=10s)
	go run ./cmd/harness -peers $(or $(PEERS),8) -for $(or $(FOR),10s) $(HARNESS_FLAGS)

# NF-14, measured against the real forwarding process under a CPU limit rather than
# against a stub sharing this one. Starts and stops its own node.
capacity: ## Measure concurrent call capacity (CALLS=3 PEERS=4 FOR=20s)
	./scripts/capacity.sh $(or $(CALLS),3) $(or $(PEERS),4) $(or $(FOR),20s)

# Separate from check because it starts four processes and drives a real browser.
# It needs `make up` and `make migrate` first, and a Chromium — see web/README.md.
e2e: ## Run the browser suite against a two-node stack
	./scripts/e2e.sh

check: lint test web-check ## Everything CI would run

clean: ## Remove build output
	rm -rf bin/
