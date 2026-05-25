# Distributed Benchmarking & Hosting Platform

[![CI — Smoke Test](https://github.com/Quant-Titans/distributed-bench-platform/actions/workflows/smoke.yml/badge.svg)](https://github.com/Quant-Titans/distributed-bench-platform/actions/workflows/smoke.yml)
[![CD — Build & Push](https://github.com/Quant-Titans/distributed-bench-platform/actions/workflows/build-push.yml/badge.svg)](https://github.com/Quant-Titans/distributed-bench-platform/actions/workflows/build-push.yml)
[![IaC Validate](https://github.com/Quant-Titans/distributed-bench-platform/actions/workflows/infra-validate.yml/badge.svg)](https://github.com/Quant-Titans/distributed-bench-platform/actions/workflows/infra-validate.yml)
[![Go 1.22+](https://img.shields.io/badge/go-1.22%2B-00ADD8?logo=go)](https://golang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Team:** Quant Titans · **Competition:** IICPC Summer Hackathon 2026 (May 9 – June 10, 2026)

---

## What We Built

A production-grade platform that evaluates contestant-submitted trading infrastructure
(order books, matching engines) under realistic market conditions:

1. **Accept** contestant code uploads (C++, Rust, Go binaries or source)
2. **Containerize** in strictly isolated sandboxes — seccomp allowlist, CapDrop ALL, CPU-pinned cgroups, read-only rootfs
3. **Bombard** with 1 000+ concurrent trading bots across 6 market microstructure archetypes
4. **Measure** kernel-level nanosecond latency via eBPF TC hooks — bypasses Go runtime jitter entirely
5. **Score** across 4 dimensions: throughput, tail latency, correctness, resilience
6. **Stream** results live to a React leaderboard over WebSocket

---

## 4 Killer Differentiators

### 1 · eBPF TC-hook latency (kernel nanoseconds, not userspace averages)

An eBPF program is attached as a TC ingress/egress classifier on the `sandbox_net` bridge.
Flow timestamps are stored in a `BPF_MAP_TYPE_LRU_HASH` and RTT is computed when the egress
event arrives. Events land in a `BPF_MAP_TYPE_RINGBUF` and are consumed by the Go telemetry
service with zero copy. This eliminates Go scheduler and context-switch jitter — the p99 you
see is the actual p99, not one inflated by the benchmark harness itself.

### 2 · 6 market microstructure bot archetypes

| Archetype | Count | Model |
|---|---|---|
| `MarketMakerBot` | 250 / 1 000 | Avellaneda-Stoikov: `r = s − q·γ·σ²·(T−t)`, spread `= γσ²(T−t) + (2/γ)ln(1+γ/κ)` |
| `NoiseBot` | 300 / 1 000 | Uniform-random Limit orders, baseline market activity |
| `MomentumBot` | 200 / 1 000 | Short/long MA crossover, Market orders on signal |
| `InstitutionalSlicerBot` | 100 / 1 000 | TWAP iceberg — 500-unit parent → 10-unit slices with 50–200 ms jitter |
| `LatencyArbBot` | 100 / 1 000 | Aggressive Market orders at 5 000 TPS (ticker-rate-capped) |
| `FIXNoiseBot` | 50 / 1 000 | FIX 4.4 wire protocol over TCP — Logon → NOS → ExecReport RTT |

All 1 000 bots share a single `*http.Transport` (MaxConnsPerHost=1024, MaxIdleConnsPerHost=1024)
and a single Kafka writer (BatchSize=256, Async=true, BatchTimeout=5 ms) to prevent fd exhaustion
and producer congestion at scale.

### 3 · 4-dimension composite scoring

```
Score = 0.30 × Throughput  +  0.30 × TailLatency  +  0.25 × Correctness  +  0.15 × Resilience
```

| Dimension | Measurement |
|---|---|
| **Throughput** | `filled_orders / elapsed_seconds`, normalized to 10 000 TPS ceiling |
| **Tail Latency** | eBPF p99 RTT (falls back to app-layer p99 if eBPF unavailable), normalized to 1 ms target |
| **Correctness** | `1 − (violations / total_fills)` — price-time priority + fill accuracy validator |
| **Resilience** | `1 − chaosP99 / baselineP99` — measured during live `tc netem` fault injection |

Scores are computed by `telemetry/internal/scorer/composite.go`, stored in TimescaleDB hypertables,
and pushed to the leaderboard via a Kafka `bench.scores` topic.

### 4 · Deterministic replay

Every order sent to a contestant engine is written to Kafka `bench.replay_log` with a monotonic
`replay_seq`. A benchmark run can be replayed bit-for-bit by re-consuming from seq=0, enabling
reproducible auditing of any submission.

---

## Architecture

```
                           ┌─────────────────────────────────────────────────────────────────────┐
                           │                     AWS EKS  eu-north-1                             │
                           │                                                                     │
  ┌──────────────────┐     │  ┌──────────────────────┐  gRPC:SpawnBotFleet  ┌─────────────────┐ │
  │  Contestant      │     │  │  Sandbox Engine      │ ──────────────────► │  Bot Fleet      │ │
  │  Upload          │ ──► │  │  [port 8080]         │                      │  [port 9090]    │ │
  │  (binary/source) │     │  │                      │ ◄────────────────── │                 │ │
  └──────────────────┘     │  │  • seccomp allowlist │  contestant endpoint │ • 1000+ bots    │ │
                           │  │  • CPU pinning       │                      │ • FIX/REST/WS   │ │
                           │  │  • read-only rootfs  │                      │ • 6 archetypes  │ │
                           │  │  • eBPF TC hooks     │                      │                 │ │
                           │  │  • tc netem chaos    │                      └────────┬────────┘ │
                           │  └──────────┬───────────┘                               │          │
                           │             │ telemetry.kernel_latency                  │          │
                           │             │ (eBPF ring buffer)              bench.raw_metrics    │
                           │             ▼                                 bench.replay_log     │
                           │  ┌──────────────────────┐                               │          │
                           │  │  Telemetry Engine    │ ◄─────────────────────────────┘          │
                           │  │                      │                                          │
                           │  │  • HDR histograms    │  bench.scores                            │
                           │  │  • TimescaleDB write │ ──────────────────────────────────────► │
                           │  │  • Chaos event score │                              ┌──────────┐│
                           │  │  • PTP validator     │                              │Leaderboard│
                           │  └──────────────────────┘                              │[port 8082]│
                           │                                                        │           │
                           │  ┌─────────────────┐  ┌──────────────────────┐        │ React +WS │
                           │  │  Redpanda        │  │  TimescaleDB (PG16)  │        └──────────┘│
                           │  │  (Kafka-compat.) │  │  Redis               │                   │
                           │  └─────────────────┘  └──────────────────────┘                   │
                           └─────────────────────────────────────────────────────────────────────┘
```

### Kafka Topics

| Topic | Producer | Consumer |
|---|---|---|
| `bench.raw_metrics` | botfleet (per order) | telemetry scorer |
| `bench.replay_log` | botfleet (every order + `replay_seq`) | replay service |
| `telemetry.kernel_latency` | sandbox eBPF prober | telemetry scorer |
| `bench.scores` | telemetry scorer (per update) | leaderboard WS server |
| `bench.events` | sandbox chaos injector | telemetry scorer |

### Service Ports

| Service | Port | Protocol |
|---|---|---|
| sandbox | 8080 | HTTP (REST) |
| botfleet | 9090 | gRPC |
| telemetry | (internal) | Kafka consumer |
| leaderboard | 8082 | HTTP + WebSocket |
| Redpanda | 9092 | Kafka |
| TimescaleDB | 5432 | PostgreSQL |
| Redis | 6379 | Redis |

---

## One-Command Deploy (AWS EKS)

```bash
# 1. Prerequisites
brew install terraform awscli helm
aws configure   # set Access Key ID, Secret, region eu-north-1

# 2. Configure secrets
cp infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars
$EDITOR infra/terraform/terraform.tfvars   # fill ghcr_token, db_password

# 3. Deploy everything (VPC + EKS + Helm + all services)
make deploy

# 4. Watch it come up
make status
```

`make deploy` runs four steps in sequence:
1. `terraform init && terraform apply` — provisions VPC, EKS cluster (c6i.2xlarge sandbox nodes + m6i.xlarge general), EBS CSI driver, IRSA roles
2. `aws eks update-kubeconfig` — writes kubeconfig
3. `helm dependency update` — pulls Redpanda chart
4. `helm upgrade --install quant-titans` — deploys all 4 services + TimescaleDB StatefulSet + Redis StatefulSet

To tear down completely: `make destroy`

---

## Local Development

```bash
# Start all services (Redpanda, TimescaleDB, Redis, all 4 services)
make up

# Full smoke test — submits a test order, checks scores appear on leaderboard
make smoke

# Run all Go tests
make test

# Regenerate gRPC Go bindings from .proto files
make proto

# Compile eBPF C program (requires Linux — runs in Docker)
make ebpf-build

# Build all Go binaries
make build
```

---

## Repository Structure

```
distributed-bench-platform/
├── sandbox/                   # Sandboxing engine
│   ├── internal/
│   │   ├── manager/           # Docker container lifecycle + eBPF prober startup
│   │   ├── chaos/             # tc netem fault injector + cgroup CPU throttle
│   │   └── ebpf/              # eBPF loader, TC hook, ring buffer consumer
│   │       └── bpf/           # tc_latency.c — TC ingress/egress program
│   └── seccomp/profile.json   # Syscall allowlist (defaultAction: SCMP_ACT_ERRNO)
│
├── botfleet/                  # Distributed bot fleet
│   └── internal/worker/
│       ├── archetypes.go      # MarketMaker, Momentum, Noise, Slicer, LatArb
│       ├── fix_client.go      # FIX 4.4 client — TCP Logon → NOS → ExecReport RTT
│       └── fleet.go           # Shared transport, batch Kafka publisher, 1000-bot spawn
│
├── telemetry/                 # Scoring + validation engine
│   └── internal/
│       ├── scorer/
│       │   ├── hdr.go         # HDR histogram — p50/p90/p99/p999
│       │   ├── validator.go   # Price-time priority + fill accuracy
│       │   └── composite.go   # 4-dimension score formula + chaos resilience
│       ├── consumer/          # Kafka reader for raw_metrics, kernel_latency, events
│       └── store/             # Async TimescaleDB writer (buffered channel 8192)
│
├── leaderboard/               # React + Go WebSocket server
│   ├── src/                   # Vite + TypeScript React app
│   └── server/                # Go WS server — Kafka bench.scores → browser push
│
├── infra/
│   ├── terraform/             # AWS EKS VPC IaC
│   └── helm/platform/         # Umbrella Helm chart (all services + data stores)
│
├── proto/                     # gRPC protobuf service definitions
├── gen/proto/                 # Generated Go bindings (committed)
├── docs/                      # Architecture blueprint + ADRs
│   ├── architecture.md
│   └── adr/
├── dummy-engine/              # Minimal Go matching engine for local testing
├── docker-compose.yml
└── Makefile
```

---

## Scoring Deep Dive

### Tail Latency (30% of score)

The telemetry service maintains two independent HDR histograms per session:
- **Kernel histogram** — fed by eBPF ring buffer events (nanosecond resolution)
- **App histogram** — fed by bot-measured RTT including Go scheduler latency

`KernelPercentiles()` returns from the kernel histogram if ≥100 samples exist, otherwise
falls back to the app histogram. The score normalizes kernel p99 against a 1 ms target:
`TailLatencyScore = clamp(1 − kernelP99ns/1_000_000, 0, 1)`.

### Resilience (15% of score)

During each benchmark run the sandbox injects 10 seconds of `tc netem delay 50ms loss 5%`
followed by 10 seconds of cgroup CPU throttle (10% of one core) after a 30-second baseline
window. The telemetry engine records `baselineP99` and `chaosP99` from chaos events:
`ResilienceScore = clamp(1 − chaosP99/baselineP99, 0, 1)`.

A submission that degrades gracefully (or recovers quickly) scores close to 1.0. One that
completely falls over scores 0.

### Correctness (25% of score)

The `PriceTimePriorityValidator` maintains a sorted bid/ask book (`sort.Slice` on price
then timestamp). Every fill is checked:
- Buy fills must clear the best ask (or better)
- Sell fills must clear the best bid (or better)
- Same-price orders must fill in FIFO order (time priority)

`CorrectnessScore = 1 − (violations / totalFills)`

---

## Technology Choices & ADRs

| Decision | Choice | Rationale |
|---|---|---|
| eBPF framework | `cilium/ebpf-go` | Portable BPF loader, no kernel module, ring buffer support |
| Docker SDK | `v25.0.8+incompatible` | v26 has macOS build tag bug in sockets.DialPipe |
| Kafka client | `segmentio/kafka-go` | Pure Go, no CGo, async batch writer |
| Latency histograms | `HdrHistogram-go` | Lock-free, lossless recording, p99.9 without distortion |
| IaC | Terraform + Helm umbrella | Single `terraform apply` — K8s provider reads EKS state source |
| Image signing | cosign keyless OIDC | Keyless — no long-lived secrets, SLSA provenance + SBOM |

---

## Performance Targets

| Metric | Target | Notes |
|---|---|---|
| Bot fleet size | 1 000+ concurrent | Single shared HTTP transport prevents fd exhaustion |
| Kafka throughput | >50 000 msg/s | Batch size 256, async, 5 ms flush |
| eBPF overhead | <2 µs per packet | TC hook, not XDP — runs after driver DMA |
| p99 kernel RTT | <500 µs | On c6i.2xlarge with CPU pinning |
| Leaderboard latency | <100 ms end-to-end | Kafka → WS push pipeline |
| Deploy time (EKS) | ~12 min from scratch | EKS cluster provisioning dominates |

---

## CI / CD

| Workflow | Trigger | What it does |
|---|---|---|
| `smoke.yml` | push/PR to main | `docker-compose up` → fire test order → assert scores appear |
| `build-push.yml` | push to main | Build all 4 images → push to GHCR → cosign sign |
| `infra-validate.yml` | push/PR to main | `terraform validate` + `helm lint` |

Images are published to:
- `ghcr.io/quant-titans/sandbox:latest`
- `ghcr.io/quant-titans/botfleet:latest`
- `ghcr.io/quant-titans/telemetry:latest`
- `ghcr.io/quant-titans/leaderboard:latest`

Each image also gets a short SHA tag (e.g., `:a1b2c3d`) for pinned deploys.

---

## Team

| Member | GitHub | Role |
|---|---|---|
| **Emmanuel Adutwum** ⭐ | `emmanueladutwum123` | **Senior Lead — Distributed Systems & Platform Architecture** · End-to-end system design and delivery: sandbox isolation engine (seccomp/namespaces), Kubernetes IaC (Helm + Terraform), eBPF kernel-level latency prober, Redpanda/Kafka telemetry pipeline, TimescaleDB ingestion, composite scoring engine, botfleet gRPC server, chaos injection framework, Cloudflare permanent tunnels, CI/CD (GitHub Actions), demo orchestration |
| Shiv Kumar Mishra | — | Distributed Systems Contributor — bot worker pool, order generation protocols |
| Pathan Farhana | — | Frontend & Scoring Contributor — React leaderboard UI, WebSocket streaming |

---

*IICPC Summer Hackathon 2026 — May 9 to June 10, 2026*
