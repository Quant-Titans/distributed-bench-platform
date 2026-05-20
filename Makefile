.PHONY: up down smoke build test sandbox-deps sandbox-build sandbox-test botfleet-build \
        leaderboard-build telemetry-build ebpf-build proto proto-clean gen-deps lint clean deploy

GOPATH_BIN := $(shell go env GOPATH)/bin
PROTO_SRC   := $(wildcard proto/*.proto)
GEN_DIR     := gen/proto

up:
	docker-compose up --build -d

down:
	docker-compose down -v

# Full stack + dummy engine smoke test
smoke:
	docker-compose --profile smoke up --build -d
	@echo "Waiting for services..."
	@sleep 10
	@echo "\n── Sandbox health ──"
	@curl -sf http://localhost:8080/healthz || echo "FAIL"
	@echo "\n── Dummy engine health ──"
	@curl -sf http://localhost:9000/healthz || echo "FAIL"
	@echo "\n── Leaderboard health ──"
	@curl -sf http://localhost:8082/healthz || echo "FAIL"
	@echo "\n── Submitting test order to dummy engine ──"
	@curl -sf -X POST http://localhost:9000/v1/order \
		-H "Content-Type: application/json" \
		-d '{"order_id":"smoke-001","symbol":"AAPL","side":"BUY","type":"LIMIT","price":150.0,"quantity":10}' \
		| python3 -m json.tool
	@echo "\n✓ Smoke test passed — open http://localhost:8082 for live leaderboard"

# ── Proto ─────────────────────────────────────────────────────────────────────
proto: proto-clean $(PROTO_SRC)
	PATH="$$PATH:$(GOPATH_BIN)" protoc \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		-I proto \
		$(PROTO_SRC)
	@echo "✓ proto generated → $(GEN_DIR)"

proto-clean:
	rm -f $(GEN_DIR)/*.go

gen-deps:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# ── Sandbox ───────────────────────────────────────────────────────────────────
sandbox-deps:
	cd sandbox && go mod tidy && go mod download

sandbox-build:
	cd sandbox && CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/sandbox ./cmd/...

sandbox-test:
	cd sandbox && go test -race -count=1 ./...

# eBPF objects compiled inside Linux Docker builder (bpf2go requires Linux kernel headers)
ebpf-build:
	docker run --rm -v "$(PWD)/sandbox:/src" -w /src \
		golang:1.26-bookworm bash -c \
		"apt-get update -qq && apt-get install -y -qq clang llvm libelf-dev linux-headers-generic && \
		 go install github.com/cilium/ebpf/cmd/bpf2go@latest && \
		 go generate ./internal/ebpf/..."

# ── Bot Fleet ─────────────────────────────────────────────────────────────────
botfleet-deps:
	cd botfleet && go mod tidy && go mod download

botfleet-build:
	cd botfleet && CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/botfleet ./cmd/...

botfleet-test:
	cd botfleet && go test -race -count=1 ./...

# ── Telemetry ─────────────────────────────────────────────────────────────────
telemetry-deps:
	cd telemetry && go mod tidy && go mod download

telemetry-build:
	cd telemetry && CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/telemetry ./cmd/...

telemetry-test:
	cd telemetry && go test -race -count=1 ./...

# ── All ───────────────────────────────────────────────────────────────────────
# ── Leaderboard ──────────────────────────────────────────────────────────────
leaderboard-deps:
	cd leaderboard && npm ci
	cd leaderboard && go mod download

leaderboard-build: leaderboard-deps
	cd leaderboard && npm run build
	cd leaderboard && CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/leaderboard-server ./server/

leaderboard-test:
	cd leaderboard && npm run build

build: sandbox-build botfleet-build telemetry-build leaderboard-build

test: sandbox-test botfleet-test telemetry-test

lint:
	golangci-lint run ./...

# ── One-command deploy ────────────────────────────────────────────────────────
deploy:
	cd infra/terraform && terraform init && terraform apply -auto-approve
	helm upgrade --install quant-titans infra/helm/platform \
		--namespace quant-titans --create-namespace \
		--wait --timeout 10m
	@echo "✓ Platform live"

clean:
	docker-compose down --volumes --remove-orphans
	rm -f sandbox/bin/* botfleet/bin/* telemetry/bin/*
