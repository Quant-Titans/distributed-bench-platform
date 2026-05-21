.PHONY: up down smoke build test sandbox-deps sandbox-build sandbox-test botfleet-build \
        leaderboard-build telemetry-build ebpf-build proto proto-clean gen-deps lint clean \
        deploy destroy status submission

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
	@until curl -sf http://localhost:8080/healthz >/dev/null 2>&1; do sleep 2; done && echo "✓ sandbox"
	@until curl -sf http://localhost:9091/healthz >/dev/null 2>&1; do sleep 2; done && echo "✓ botfleet"
	@until curl -sf http://localhost:8082/healthz >/dev/null 2>&1; do sleep 2; done && echo "✓ leaderboard"
	@until curl -sf http://localhost:9000/healthz >/dev/null 2>&1; do sleep 2; done && echo "✓ dummy-engine"
	@echo "\n── Submitting test order to dummy engine ──"
	@curl -sf -X POST http://localhost:9000/v1/order \
		-H "Content-Type: application/json" \
		-d '{"order_id":"smoke-001","symbol":"AAPL","side":"BUY","type":"LIMIT","price":150.0,"quantity":10}' \
		| python3 -m json.tool
	@echo "\n✓ Smoke test passed — open http://localhost:8082 for live leaderboard"

# Full end-to-end demo: start platform, upload binary, watch leaderboard update live
demo:
	docker-compose up --build -d
	@echo "Waiting for platform services..."
	@until curl -sf http://localhost:8080/healthz >/dev/null 2>&1; do sleep 2; done && echo "  ✓ sandbox  :8080"
	@until curl -sf http://localhost:9091/healthz >/dev/null 2>&1; do sleep 2; done && echo "  ✓ botfleet :9091"
	@until curl -sf http://localhost:8082/healthz >/dev/null 2>&1; do sleep 2; done && echo "  ✓ leaderboard :8082"
	@echo "\nBuilding contestant binary..."
	@cd dummy-engine && go build -o /tmp/demo-engine . && echo "  ✓ binary ready ($(du -sh /tmp/demo-engine | cut -f1))"
	@echo "\n── Team Alpha upload ──"
	@RESP=$$(curl -sf --max-time 120 -X POST http://localhost:8080/v1/upload \
		-F "team_name=Team Alpha" \
		-F "session_id=demo-alpha" \
		-F "binary=@/tmp/demo-engine" \
		-F "timeout_s=90") && \
	echo "$$RESP" | python3 -m json.tool && \
	echo "\n  ✓ Team Alpha sandbox live — bot fleet launching in background"
	@echo "\n── Team Beta upload ──"
	@RESP=$$(curl -sf --max-time 120 -X POST http://localhost:8080/v1/upload \
		-F "team_name=Team Beta" \
		-F "session_id=demo-beta" \
		-F "binary=@/tmp/demo-engine" \
		-F "timeout_s=90") && \
	echo "$$RESP" | python3 -m json.tool && \
	echo "\n  ✓ Team Beta sandbox live — bot fleet launching in background"
	@echo "\n════════════════════════════════════════════"
	@echo "  Live leaderboard → http://localhost:8082"
	@echo "  (scores appear within ~5 seconds)"
	@echo "════════════════════════════════════════════\n"

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
# Prerequisites: terraform ≥1.6, aws CLI (authenticated), helm ≥3.14, kubectl
deploy:
	@echo "==> [1/4] Provisioning VPC + EKS cluster (terraform apply)..."
	cd infra/terraform && terraform init -input=false && terraform apply -auto-approve -input=false
	@echo "==> [2/4] Configuring kubectl for the new cluster..."
	aws eks update-kubeconfig --name quant-titans --region eu-north-1
	@echo "==> [3/4] Updating Helm chart dependencies (Redpanda chart)..."
	helm repo add redpanda https://charts.redpanda.com 2>/dev/null || true
	helm dependency update infra/helm/platform
	@echo "==> [4/4] Deploying platform Helm chart..."
	helm upgrade --install quant-titans infra/helm/platform \
		--namespace quant-titans --create-namespace \
		--wait --timeout 15m
	@echo ""
	@echo "✓ Platform deployed. Run 'make status' to verify."

destroy:
	cd infra/terraform && terraform destroy -auto-approve -input=false
	@echo "✓ All AWS resources destroyed."

status:
	@echo "── Pods ──────────────────────────────────────────────────────────"
	@kubectl get pods -n quant-titans
	@echo ""
	@echo "── Services ──────────────────────────────────────────────────────"
	@kubectl get svc -n quant-titans
	@echo ""
	@echo "── Leaderboard URL ───────────────────────────────────────────────"
	@kubectl get svc leaderboard -n quant-titans \
		-o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null \
		|| echo "(LoadBalancer still provisioning — retry in 2 min)"
	@echo ""

# ── Submission packaging ─────────────────────────────────────────────────────
SUBMISSION_TAG  := quant-titans-$(shell date +%Y%m%d)
SUBMISSION_DIR  := dist/$(SUBMISSION_TAG)
SUBMISSION_TGZ  := dist/$(SUBMISSION_TAG).tar.gz

submission:
	@echo "Packaging IICPC submission — $(SUBMISSION_TAG)..."
	@rm -rf dist/ && mkdir -p $(SUBMISSION_DIR)
	@# Source snapshot from current git HEAD (excludes untracked/ignored files)
	@git archive HEAD --format=tar | tar -x -C $(SUBMISSION_DIR)
	@# Always include the docs tree even if not committed on this branch
	@cp -r docs $(SUBMISSION_DIR)/
	@# Write a machine-readable manifest
	@printf 'team: Quant Titans\ncompetition: IICPC Summer Hackathon 2026\ncommit: %s\ndate: %s\n' \
	    "$$(git rev-parse HEAD)" "$$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
	    > $(SUBMISSION_DIR)/MANIFEST.txt
	@# Sanity checks — required deliverables must be present
	@test -f $(SUBMISSION_DIR)/Makefile           && echo "  ✓ Makefile"          || (echo "FAIL: Makefile missing" && exit 1)
	@grep -q '^deploy:'     $(SUBMISSION_DIR)/Makefile && echo "  ✓ make deploy"  || (echo "FAIL: make deploy target missing" && exit 1)
	@grep -q '^submission:' $(SUBMISSION_DIR)/Makefile && echo "  ✓ make submission" || (echo "FAIL: make submission target missing" && exit 1)
	@test -f $(SUBMISSION_DIR)/docker-compose.yml && echo "  ✓ docker-compose.yml" || (echo "FAIL: docker-compose.yml missing" && exit 1)
	@test -f $(SUBMISSION_DIR)/docs/architecture.md && echo "  ✓ architecture.md" || (echo "FAIL: docs/architecture.md missing" && exit 1)
	@test -f $(SUBMISSION_DIR)/README.md          && echo "  ✓ README.md"         || (echo "FAIL: README.md missing" && exit 1)
	@test -d $(SUBMISSION_DIR)/infra/terraform    && echo "  ✓ infra/terraform"   || (echo "FAIL: infra/terraform missing" && exit 1)
	@test -d $(SUBMISSION_DIR)/infra/helm         && echo "  ✓ infra/helm"        || (echo "FAIL: infra/helm missing" && exit 1)
	@# Pack
	@cd dist && tar -czf $(SUBMISSION_TAG).tar.gz $(SUBMISSION_TAG)/
	@echo ""
	@echo "════════════════════════════════════════════"
	@echo "  Submission ready: $(SUBMISSION_TGZ)"
	@echo "  Size: $$(du -sh $(SUBMISSION_TGZ) | cut -f1)"
	@echo "════════════════════════════════════════════"

clean:
	docker-compose down --volumes --remove-orphans
	rm -f sandbox/bin/* botfleet/bin/* telemetry/bin/*
	rm -rf dist/
