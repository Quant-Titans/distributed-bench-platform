# Architecture — Quant Titans Distributed Benchmarking Platform

**Team:** Quant Titans | **Competition:** IICPC Summer Hackathon 2026  
**Last updated:** 2026-05-13 (Week 1 — Sandbox POC)

> This document grows incrementally each sprint. Sections marked **[Week N]** indicate when that part will be fleshed out.

---

## 1. System Overview

The platform evaluates contestant-submitted trading infrastructure (order books, matching engines) by running each submission inside a hardened sandbox, bombarding it with a distributed fleet of trading bots, capturing granular telemetry, and streaming results to a live leaderboard.

**Key engineering goals (judging criteria):**
- Deep sandboxing — seccomp allowlist, dropped Linux capabilities, CPU-pinned cgroups, isolated network namespace
- Real tail-latency measurement — HDR histogram p50/p90/p99, never averages
- FIX protocol support — signals trading domain knowledge
- Horizontal scalability — 1000+ concurrent bots without message loss
- One-command reproducible deploy — `terraform apply` from scratch

---

## 2. High-Level Architecture

Four decoupled microservices communicate via gRPC (synchronous control plane) and Redpanda/Kafka (asynchronous metrics bus):

```
┌──────────────────────────────────┐        gRPC: SpawnBotFleet       ┌────────────────────────┐
│  Sandbox Engine          [8080]  │ ───────────────────────────────► │  Bot Fleet             │
│  Emmanuel                        │                                   │  Shiv Kumar            │
│                                  │ ◄─────────────────────────────── │                        │
│  • Accepts binary/image upload   │        gRPC: ContestantEndpoint  │  • 1000+ Go goroutines │
│  • Docker + seccomp isolation    │                                   │  • FIX / WS / REST     │
│  • CPU pinning, memory limits    │                                   │  • Market + Limit +    │
│  • Network namespace per sub.    │                                   │    Cancel orders       │
└──────────┬───────────────────────┘                                   └───────────┬────────────┘
           │                                                                       │
           │ endpoint URL (gRPC)                              metrics (Redpanda)   │
           ▼                                                                       ▼
┌──────────────────────────────────┐        scores (gRPC / WS)        ┌────────────────────────┐
│  Telemetry & Validation          │ ───────────────────────────────► │  Real-Time Leaderboard │
│  Shiv Kumar                      │                                   │  Farhana               │
│                                  │                                   │                        │
│  • HDR histogram p50/p90/p99     │                                   │  • React + WebSocket   │
│  • TimescaleDB hypertables       │                                   │  • Composite scoring   │
│  • Redis leaderboard state       │                                   │  • Live updates        │
│  • Price-time priority validator │                                   │  • Upload UI           │
│  • Fill accuracy checker         │                                   │                        │
└──────────────────────────────────┘                                   └────────────────────────┘
```

### End-to-End Data Flow

```
Contestant uploads binary/image
         │
         ▼
  [Sandbox Engine] validates image, spins up isolated container
         │  gRPC SpawnBotFleet(endpoint, session_id)
         ▼
  [Bot Fleet] goroutines send FIX/WS/REST orders at target TPS
         │  Redpanda topic: bench.raw_metrics
         ▼
  [Telemetry] consumes, validates price-time priority, computes HDR histograms
         │  Redpanda topic: bench.scores  +  Redis ZADD leaderboard
         ▼
  [Leaderboard] WebSocket push to browser — score updates in <1s
```

---

## 3. Sandbox Engine (Emmanuel — `sandbox/`)

### 3.1 Responsibility

The Sandbox Engine is the security-critical gatekeeper of the platform. It:

1. Accepts a contestant image (Docker image name or uploaded binary) via HTTP POST
2. Spins up a strictly isolated Docker container for that submission
3. Enforces resource limits, syscall policy, and network isolation
4. Returns the container's internal endpoint so the Bot Fleet can target it
5. Enforces a hard time limit and tears down the container when the session ends

### 3.2 Security Model — Defense in Depth

Security is layered so that a compromised contestant binary cannot escape the sandbox:

| Layer | Mechanism | What it prevents |
|---|---|---|
| Syscall filtering | Custom seccomp allowlist (`seccomp/profile.json`) | Raw sockets, ptrace, mount, kernel module loading, kexec |
| Linux capabilities | `CapDrop: ALL`, `CapAdd: NET_BIND_SERVICE` only | Privilege escalation, chroot escapes, raw I/O |
| No new privileges | `no-new-privileges:true` security opt | setuid/setgid binaries gaining root |
| Read-only root FS | `ReadonlyRootfs: true` + `/tmp` tmpfs | Persistence of malicious files across restarts |
| Network isolation | Dedicated `sandbox_net` bridge, `enable_icc=false` | Lateral movement between contestant containers |
| Resource limits | cgroup v2 — `memory`, `cpuset` | CPU steal, memory exhaustion, noisy-neighbour |
| Hard timeout | Go context with `WithTimeout` | Hung submissions blocking slots indefinitely |

#### Seccomp Profile Design

The profile (`seccomp/profile.json`) uses **`defaultAction: SCMP_ACT_ERRNO`** — a strict allowlist that denies every syscall not explicitly permitted. Allowed categories:

- **File I/O** — read, write, open, stat, dup, fcntl, epoll, inotify (for config + log files)
- **Networking** — socket, bind, connect, listen, accept, send/recvmsg (for serving the order API)
- **Memory** — mmap, mprotect, brk, mlock (legitimate allocator and JIT use)
- **Threading** — clone, futex, set_tid_address, arch_prctl (Go/C++ runtime needs)
- **Signals** — rt_sigaction, kill (intra-process only)
- **Time** — clock_gettime, nanosleep, timerfd (latency measurement from inside)
- **Process** — execve (initial start only), exit_group, wait4

Explicitly denied (default action): `mount`, `umount2`, `ptrace`, `kexec_load`, `init_module`, `perf_event_open`, `process_vm_readv`, `bpf`, `capset`, `seccomp`.

### 3.3 API

```
POST   /v1/sandbox/run          — launch a contestant container
GET    /v1/sandbox/{id}/status  — poll container state
DELETE /v1/sandbox/{id}         — stop and remove container
GET    /healthz                 — liveness check
```

**POST /v1/sandbox/run — Request:**
```json
{
  "image":     "contestant/matching-engine:v1",
  "cpu_cores": "0-1",
  "memory_mb": 512,
  "timeout_s": 120,
  "env":       ["LOG_LEVEL=info"]
}
```

**Response (201):**
```json
{
  "id":         "a3f9c1d2b4e6",
  "image":      "contestant/matching-engine:v1",
  "endpoint":   "http://172.20.0.3:8080",
  "status":     "running",
  "started_at": "2026-05-13T10:04:00Z"
}
```

### 3.4 Container Lifecycle

```
POST /v1/sandbox/run
       │
       ├─ validate config (image, cpu, memory)
       ├─ ContainerCreate (seccomp, capDrop, readonlyRootfs, cpuset, memory)
       ├─ ContainerStart
       ├─ ContainerInspect → get IP on sandbox_net
       ├─ spawn enforceTimeout goroutine (context.WithTimeout)
       └─ return SandboxInfo {id, endpoint, status}

enforceTimeout goroutine:
  <-ctx.Done()  →  ContainerStop (SIGTERM, 2s grace)  →  ContainerRemove(Force)
```

### 3.5 Week 2 Additions (planned)

- Binary upload endpoint (`POST /v1/sandbox/upload`) — build image from uploaded tarball
- CPU pinning via `cpuset` mapped to dedicated cores from a pool
- gRPC service (`proto/sandbox.proto`) replacing the HTTP API for internal comms
- Kubernetes `Job` resource for contestant containers instead of bare Docker (Week 2 IaC)

---

## 4. Bot Fleet (`botfleet/`) — [Week 1 skeleton, detail Week 2]

Shiv Kumar's service. Spawns up to 1000+ Go goroutines, each simulating an independent trading participant.

**Order types generated:** Limit, Market, Cancel  
**Protocols:** FIX (QuickFIX/Go), WebSocket, REST (fallback)  
**Metrics produced to Redpanda:** `bench.raw_metrics` topic — per-order latency, fill result, timestamp

Detailed design: TBD Week 2.

---

## 5. Telemetry & Validation (`telemetry/`) — [Week 1 skeleton, detail Week 2]

Shiv Kumar's service. Consumes `bench.raw_metrics` from Redpanda, validates correctness, and computes HDR histograms.

**Latency metrics:** p50 / p90 / p99 per session — stored in TimescaleDB hypertables  
**Correctness validation:** price-time priority check on every fill event; fill accuracy ratio  
**Hot-path state:** Redis ZADD for leaderboard ranking (score = composite of latency + TPS + accuracy)

Detailed design: TBD Week 2.

---

## 6. Real-Time Leaderboard (`leaderboard/`) — [Week 1 scaffold, detail Week 2]

Farhana's service. React frontend, native WebSocket, composite scoring display.

Detailed design: TBD Week 2.

---

## 7. Inter-Service Communication

### gRPC (control plane)

| Call | From | To | Description |
|---|---|---|---|
| `SpawnBotFleet` | Sandbox | Bot Fleet | Start load test against `endpoint` |
| `StopBotFleet` | Sandbox | Bot Fleet | Tear down bots when sandbox stops |
| `PublishScore` | Telemetry | Leaderboard | Push updated composite score |

Proto definitions live in `proto/` — all services share this directory.

### Redpanda (data plane)

| Topic | Producer | Consumer | Message |
|---|---|---|---|
| `bench.raw_metrics` | Bot Fleet | Telemetry | Per-order: latency_ns, order_type, fill_result, ts |
| `bench.scores` | Telemetry | Leaderboard | Per-session: p50/p90/p99, tps, accuracy, composite |
| `bench.events` | Sandbox | All | Sandbox lifecycle events (start, stop, timeout) |

---

## 8. Data Stores

| Store | Service | Usage |
|---|---|---|
| TimescaleDB | Telemetry | Hypertable `order_metrics(session_id, ts, latency_ns, order_type, fill_ok)` |
| Redis | Telemetry + Leaderboard | `ZADD leaderboard {score} {session_id}` — sorted set for O(log n) ranking |

---

## 9. Deployment Model — [Week 2 IaC, detail then]

**Target:** AWS EKS (eu-north-1), one-command deploy via `terraform apply`.

High-level Terraform module structure (planned):
- `infra/terraform/eks.tf` — EKS cluster, node groups (sandbox-nodes, botfleet-nodes)
- `infra/terraform/networking.tf` — VPC, subnets, security groups
- `infra/helm/sandbox/` — sandbox engine Helm chart
- `infra/helm/botfleet/` — bot fleet Helm chart with HPA
- `infra/helm/telemetry/` — TimescaleDB + Redis + ingester
- `infra/helm/leaderboard/` — React app + WebSocket server

Detailed IaC design: TBD Week 2.

---

## 10. Technology Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Sandbox runtime | Docker + host socket | DinD simplest for POC; K8s Jobs for production (Week 2) |
| Seccomp approach | Allowlist (`defaultAction: ERRNO`) | Deny-by-default is the only safe baseline for untrusted code |
| Bot fleet language | Go | Goroutine-per-bot at 1000+ scale; GC pauses acceptable (not on hot path); no GIL |
| Messaging bus | Redpanda | Kafka-compatible, lower latency, single binary — reduces infra complexity |
| Latency measurement | HDR histograms | Captures tail latency truthfully; averages hide p99 blowout |
| Metrics DB | TimescaleDB | SQL + time-series in one; hypertables compress and partition by time automatically |
| Order protocols | FIX + WS + REST | FIX is the industry standard — shows domain knowledge to judges |
| IaC | Terraform + Helm | Reproducible, diff-able, supports one-command deploy requirement |

---

## 11. Architecture Decision Records

| ADR | Decision |
|---|---|
| [ADR-001](adr/001-seccomp-allowlist.md) | Use allowlist seccomp (`defaultAction: ERRNO`) over Docker default blocklist |

---

*Document maintainer: Emmanuel Adutwum. Updated each sprint — do not leave this as a last-minute writeup.*
