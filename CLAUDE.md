# CLAUDE.md — Quant Titans / IICPC Summer Hackathon 2026

This file gives Claude Code full context about this project so it can assist
effectively across all sessions without needing re-explanation.

---

## Who We Are

**Team:** Quant Titans
**GitHub Org:** https://github.com/Quant-Titans
**Repo:** https://github.com/Quant-Titans/distributed-bench-platform
**Competition:** IICPC Summer Hackathon 2026

### Team Members

| Name | GitHub | Role |
|---|---|---|
| Emmanuel Adutwum | emmanueladutwum123 | Infrastructure Architect — sandbox, IaC, AWS, telemetry pipeline, architecture doc |
| Shiv Kumar Mishra | (confirm username) | Distributed Systems Lead — bot fleet, gRPC, Kafka, latency measurement, correctness validation |
| Pathan Farhana | (confirm username) | Frontend & Scoring Lead — React leaderboard, WebSocket streaming, composite scoring, upload UI |

---

## Competition Details

**Event:** IICPC Summer Hackathon 2026
**Duration:** May 9 – June 10, 2026 (submissions open during final week, June 4–10)
**Team size:** Max 3 people ✅
**Format:** Not a demo-to-win hackathon. Judges evaluate hardcore engineering:
high-performance code, system resilience, deep understanding of scale and
distributed systems. AI tools are permitted but every architectural decision
must be deliberate and well-reasoned.

### The Challenge

Build a **Distributed Benchmarking and Hosting Platform** that evaluates
contestant-submitted trading infrastructure (e.g. order books, matching engines).

The platform must:
1. Accept contestant code uploads (C++, Rust, Go binaries or source)
2. Containerize and deploy submissions in strictly isolated sandboxes
3. Expose predefined API/WebSocket endpoints for the submission
4. Dynamically spawn a massive distributed fleet of trading bots
5. Bombard the contestant's system with concurrent orders (Limit, Market, Cancel)
6. Capture granular telemetry: latency (p50/p90/p99), throughput (TPS), correctness
7. Stream results to a live dynamic leaderboard

### Required Deliverables

1. **Working Infrastructure Prototype** — full pipeline: Upload → Containerized
   Deployment → Distributed Load Testing → Real-Time Scoring
2. **Architecture Blueprint** — microservices design doc, inter-service protocols
   (gRPC, Kafka/Redpanda), data stores (TimescaleDB, Redis), isolation strategies
3. **Infrastructure as Code (IaC)** — Terraform + Kubernetes manifests or Docker
   Swarm; platform must spin up, configure, and scale horizontally in one command

### Judging Criteria (inferred from spec)

- Sandboxing depth and security (seccomp, CPU pinning, network isolation)
- Correctness of price-time priority validation and fill accuracy
- Real tail latency measurement (p50/p90/p99 HDR histograms, not averages)
- FIX protocol support (signals domain knowledge)
- Horizontal scalability and one-command reproducible deploy
- Quality of architecture blueprint document
- Live demo impact (real-time leaderboard updating during presentation)

---

## System Architecture

Four decoupled microservices communicating via gRPC and an event bus:

```
┌─────────────────────┐         deploy/spin up        ┌─────────────────────┐
│  Submission &       │ ────────────────────────────► │  Distributed        │
│  Sandboxing Engine  │                               │  Bot Fleet          │
│  [Emmanuel]         │                               │  [Shiv Kumar]       │
└────────┬────────────┘                               └──────────┬──────────┘
         │ endpoint URL                          orders (FIX/WS) │  metrics (Kafka)
         ▼                                                       ▼
┌─────────────────────┐        scores                ┌─────────────────────┐
│  Telemetry &        │ ────────────────────────────► │  Real-Time          │
│  Validation         │                               │  Leaderboard        │
│  [Shiv Kumar]       │                               │  [Farhana]          │
└─────────────────────┘                               └─────────────────────┘
```

---

## Repository Structure

```
distributed-bench-platform/
├── sandbox/           # Emmanuel — Docker/K8s sandboxing engine
│   ├── Dockerfile
│   ├── sandbox_manager.go
│   └── seccomp_profile.json
├── botfleet/          # Shiv Kumar — Go/Rust distributed load generator
│   ├── cmd/
│   ├── internal/
│   │   ├── fix/       # FIX protocol order generation
│   │   ├── ws/        # WebSocket order generation
│   │   └── worker/    # Bot worker pool
│   └── proto/
├── telemetry/         # Shiv Kumar — ingester, TimescaleDB, Redis
│   ├── ingester.go
│   ├── validator.go   # price-time priority + fill accuracy
│   └── metrics/
├── leaderboard/       # Farhana — React + WebSocket frontend
│   ├── src/
│   │   ├── components/
│   │   └── hooks/useWebSocket.ts
│   └── package.json
├── infra/             # Emmanuel — Terraform + Helm charts
│   ├── terraform/
│   │   ├── main.tf
│   │   ├── eks.tf
│   │   └── variables.tf
│   └── helm/
│       ├── sandbox/
│       ├── botfleet/
│       ├── telemetry/
│       └── leaderboard/
├── proto/             # Shared gRPC protobuf definitions (all three touch this)
│   ├── sandbox.proto
│   ├── botfleet.proto
│   └── telemetry.proto
├── docs/              # Architecture blueprint + design decisions
│   ├── architecture.md
│   └── adr/           # Architecture Decision Records
├── docker-compose.yml # Local dev — all four services wired together
├── Makefile           # Common commands: make up, make test, make lint
├── README.md
└── CLAUDE.md          # This file
```

---

## Technology Stack

| Layer | Choice | Reason |
|---|---|---|
| Sandbox / Isolation | Docker + Kubernetes | seccomp, namespaces, CPU pinning |
| Bot Fleet | Go (goroutines) | concurrency at scale; no GC pauses on latency paths |
| Inter-service comms | gRPC + Protocol Buffers | streaming, strongly typed, low overhead |
| Messaging bus | Redpanda | Kafka-compatible, lower latency, single binary |
| Metrics DB | TimescaleDB | time-series hypertables, full SQL |
| Hot-path cache | Redis | leaderboard state, active session tracking |
| IaC | Terraform + Helm | reproducible one-command deploys |
| Order protocols | FIX + REST + WebSocket | FIX signals domain knowledge |
| Frontend | React + native WebSocket | real-time streaming |
| Cloud | AWS EC2/EKS (eu-north-1) | Emmanuel's existing infra familiarity |

---

## Branch Strategy

```
main              ← protected; requires 1 PR approval before merge
feat/sandbox-*    ← Emmanuel's branches
feat/botfleet-*   ← Shiv Kumar's branches
feat/leaderboard-* ← Farhana's branches
feat/infra-*      ← Emmanuel's branches
fix/*             ← bug fixes (any member)
docs/*            ← documentation updates
```

**Never push directly to `main`.** Always open a PR; at least one teammate reviews.

---

## Sprint Timeline

### Week 1 — May 9–16: Skeleton & Contracts
- [ ] Monorepo structure initialized (Emmanuel)
- [ ] Docker sandbox proof-of-concept (Emmanuel)
- [ ] gRPC protobuf service contracts defined (Shiv Kumar)
- [ ] Bot worker prototype — 50 bots, REST orders (Shiv Kumar)
- [ ] Redpanda/Kafka topic design (Shiv Kumar)
- [ ] Upload UI wireframe + WebSocket scaffold (Farhana)
- [ ] **Milestone:** End-to-end smoke test — 5 bots → dummy endpoint, latency measured

### Week 2 — May 17–25: Core Pipeline Integration
- [ ] K8s manifests + Terraform modules (Emmanuel)
- [ ] CPU pinning + seccomp isolation (Emmanuel)
- [ ] Kafka/Redpanda → TimescaleDB metrics bus (Shiv Kumar)
- [ ] HDR histogram latency (p50/p90/p99) (Shiv Kumar)
- [ ] Price-time priority correctness validator (Shiv Kumar)
- [ ] Live score feed → leaderboard (Farhana)

### Week 3 — May 26 – June 3: Scale & Stress Test
- [ ] Scale to 1000+ concurrent bots (Shiv Kumar)
- [ ] FIX protocol + WebSocket order diversity (Shiv Kumar)
- [ ] Horizontal scaling validation (Emmanuel)
- [ ] One-command `terraform apply` end-to-end (Emmanuel)
- [ ] Composite score algorithm finalized (Farhana)
- [ ] Architecture document first complete draft (all)

### Week 4 — June 4–10: Polish & Submit
- [ ] Architecture blueprint finalized (all)
- [ ] Leaderboard live demo recorded (Farhana)
- [ ] One-command deploy tested twice from scratch (Emmanuel)
- [ ] Submission packaged and submitted

---

## Key Engineering Constraints

1. **No direct pushes to `main`** — always via PR with review
2. **Bot fleet must be Go or Rust** — Python will not handle the concurrency
3. **Latency must use HDR histograms** — never report only averages; always p50/p90/p99
4. **FIX protocol is required** — use QuickFIX/Go library
5. **One-command deploy is a hard deliverable** — `terraform apply` must spin up
   the entire platform from scratch; test this at minimum twice
6. **Correctness validation is non-negotiable** — price-time priority and fill
   accuracy checks must be implemented and visible in the leaderboard UI
7. **Architecture doc is a graded deliverable** — maintain `docs/architecture.md`
   incrementally from Week 1; it is not a last-minute writeup

---

## Local Dev Quick Start

```bash
# Clone
git clone https://github.com/Quant-Titans/distributed-bench-platform
cd distributed-bench-platform

# Start all services locally
docker-compose up

# Run all tests
make test

# Regenerate gRPC stubs from proto files
make proto

# Lint all services
make lint

# Deploy to AWS (Week 3+)
cd infra/terraform && terraform apply
```

---

## Contact & Links

- **Repo:** https://github.com/Quant-Titans/distributed-bench-platform
- **Project Board:** https://github.com/orgs/Quant-Titans/projects/1
- **Emmanuel:** emmanueladutwum900@yahoo.com | (929) 377-7654
- **Competition:** IICPC Summer Hackathon 2026 (May 9 – June 10, 2026)
