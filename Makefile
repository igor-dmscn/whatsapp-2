.PHONY: help up down migrate build lint test check clean web web-check e2e load

# Loaded so make targets see the same values the binaries do.
ifneq (,$(wildcard .env))
include .env
export
endif

help: ## Show available targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-10s %s\n", $$1, $$2}'

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

web: ## Run the browser client against a local api on :8080
	cd web && npm install --silent && npm run dev

web-check: ## Typecheck the browser client and run its unit tests
	cd web && npm install --silent && npm run build && npm test

# The media load script. Points at a local stub by default, so it is runnable with
# nothing deployed; -url points it at a real server. See internal/harness.
load: ## Drive a media server with simulated participants (PEERS=20 FOR=10s)
	go run ./cmd/harness -peers $(or $(PEERS),8) -for $(or $(FOR),10s) $(HARNESS_FLAGS)

# Separate from check because it starts four processes and drives a real browser.
# It needs `make up` and `make migrate` first, and a Chromium — see web/README.md.
e2e: ## Run the browser suite against a two-node stack
	./scripts/e2e.sh

check: lint test web-check ## Everything CI would run

clean: ## Remove build output
	rm -rf bin/
