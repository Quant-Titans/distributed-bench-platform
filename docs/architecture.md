# Architecture — Quant Titans Distributed Benchmarking Platform

**Team:** Quant Titans | **Competition:** IICPC Summer Hackathon 2026  
**Last updated:** 2026-05-20 (Week 3 — fully operational)

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [High-Level Architecture](#2-high-level-architecture)
3. [End-to-End Data Flow](#3-end-to-end-data-flow)
4. [Sandbox Engine](#4-sandbox-engine)
5. [eBPF Kernel Latency Prober](#5-ebpf-kernel-latency-prober)
6. [Bot Fleet](#6-bot-fleet)
7. [Telemetry & Validation](#7-telemetry--validation)
8. [Real-Time Leaderboard](#8-real-time-leaderboard)
9. [Chaos Engineering](#9-chaos-engineering)
10. [Inter-Service Communication](#10-inter-service-communication)
11. [Data Stores](#11-data-stores)
12. [Infrastructure as Code](#12-infrastructure-as-code)
13. [CI/CD Pipeline](#13-cicd-pipeline)
14. [Composite Scoring Algorithm](#14-composite-scoring-algorithm)
15. [Technology Decisions](#15-technology-decisions)
16. [Architecture Decision Records](#16-architecture-decision-records)
17. [Performance Characteristics](#17-performance-characteristics)

---

## 1. System Overview

The Quant Titans Distributed Benchmarking Platform evaluates contestant-submitted trading infrastructure (order books, matching engines) under realistic, high-throughput market conditions. It is designed around four engineering principles that directly map to the judging criteria:

| Principle | Realisation |
|---|---|
| **Deep sandboxing** | seccomp allowlist + dropped capabilities + cgroup v2 + isolated bridge network |
| **Truthful latency measurement** | dual-layer: app-level RTT (Go timer) + kernel-level RTT (eBPF TC hooks, no user-space overhead) |
| **Scale** | 1000+ concurrent Go goroutines with five statistically-grounded market microstructure archetypes |
| **Correctness validation** | deterministic price-time priority replay via Kafka monotonic sequence log |
| **One-command deploy** | `terraform apply` → EKS cluster + all services live in eu-north-1 |

---

## 2. High-Level Architecture

Four microservices communicate over two planes:

- **Control plane (gRPC + HTTP)** — synchronous commands: spawn sandbox, start/stop bot fleet
- **Data plane (Redpanda/Kafka)** — asynchronous high-throughput metrics, scores, replay events

```
┌──────────────────────────────────┐   gRPC: SpawnBotFleet    ┌───────────────────────────────┐
│  Sandbox Engine          :8080   │ ───────────────────────► │  Bot Fleet                    │
│                                  │                           │                               │
│  • Upload / containerise sub.    │ ◄─────────────────────── │  • 1 goroutine per bot        │
│  • seccomp + capability drop     │   gRPC: ContestantURL     │  • 5 market microstructure    │
│  • CPU pinning (cpuset cgroup)   │                           │    archetypes                 │
│  • Network namespace isolation   │                           │  • FIX / REST / WebSocket     │
│  • Chaos fault injection         │                           │  • HDR histogram recording    │
│  • eBPF TC-hook latency prober   │                           │  • Monotonic replay_seq       │
└──────────┬───────────────────────┘                           └───────────┬───────────────────┘
           │                                                               │
           │  bench.raw_metrics (Redpanda)                                 │ bench.raw_metrics
           │  telemetry.kernel_latency (Redpanda)                          │ bench.replay_log
           ▼                                                               ▼
┌──────────────────────────────────┐   bench.scores           ┌───────────────────────────────┐
│  Telemetry & Validation   :8081  │ ───────────────────────► │  Real-Time Leaderboard :8082  │
│                                  │                           │                               │
│  • HDR histograms p50/p90/p99    │                           │  • Go WebSocket hub           │
│  • 4-dimension composite score   │                           │  • React SPA                  │
│  • Price-time priority validator │                           │  • Live rank updates          │
│  • TimescaleDB writer            │                           │  • Snapshot-on-connect        │
│  • Replay correctness verifier   │                           │                               │
└──────────────────────────────────┘                           └───────────────────────────────┘

                    ┌──────────────────────────────────────┐
                    │  Infrastructure                       │
                    │  Redpanda (Kafka-compatible)          │
                    │  TimescaleDB (metrics + scores)       │
                    │  Redis (session state)                │
                    └──────────────────────────────────────┘
```

---

## 3. End-to-End Data Flow

```
1.  POST /v1/sandbox/run  →  Sandbox Engine
       │
       ├─ pulls / validates contestant image
       ├─ ContainerCreate with seccomp, capDrop, cpuset, readonlyRootfs
       ├─ ContainerStart on sandbox_net bridge (ICC disabled)
       ├─ resolveBridgeIface() → "br-<network_id>"
       ├─ runEBPFProber() goroutine — TC ingress/egress hooks on bridge iface
       └─ gRPC SpawnBotFleet(endpoint, session_id) → Bot Fleet

2.  Bot Fleet goroutines  →  contestant engine  (FIX / REST / WS)
       │
       ├─ record RTT: t_send … t_response (app-layer, Go time.Now())
       ├─ publish bench.raw_metrics → Redpanda (per order, no batching on hot path)
       ├─ publish bench.replay_log  → Redpanda (monotonic replay_seq for determinism)
       └─ eBPF TC hook independently measures kernel-level RTT (no syscall overhead)

3.  eBPF prober  →  bench.telemetry.kernel_latency (Redpanda)
       │
       └─ Sandbox Manager reads RINGBUF, encodes latency_event, publishes to Kafka

4.  Telemetry service consumes both topics:
       │
       ├─ HDR histogram update (HdrHistogram-go, no locks on hot path)
       ├─ Validator.RecordFill() → price-time priority check
       ├─ Engine.Compute() → 4-dimension composite score
       ├─ INSERT INTO composite_scores (TimescaleDB)
       └─ json.Marshal(CompositeScore) → bench.scores (Redpanda)

5.  Leaderboard server:
       ├─ Kafka consumer reads bench.scores
       ├─ Hub.upsert() → re-sort entries by TotalScore
       └─ Hub.broadcast() → wsjson.Write to all connected browser clients
```

---

## 4. Sandbox Engine

### 4.1 Responsibility

The Sandbox Engine is the security-critical gatekeeper. It accepts contestant images, enforces strict resource isolation, and coordinates the lifecycle of each evaluation session.

### 4.2 Security Model — Defense in Depth

Every layer is independently enforced; compromising one does not bypass the others.

| Layer | Mechanism | What it prevents |
|---|---|---|
| Syscall filtering | Custom seccomp allowlist, `defaultAction: SCMP_ACT_ERRNO` | Raw sockets, ptrace, mount, kexec, BPF |
| Linux capabilities | `CapDrop: ALL`, restore `NET_BIND_SERVICE` only | Privilege escalation, chroot escapes |
| No-new-privileges | `SecurityOpt: no-new-privileges:true` | setuid/setgid escalation |
| Read-only root filesystem | `ReadonlyRootfs: true` + `/tmp` tmpfs overlay | Persistent malicious file writes |
| Network isolation | Dedicated `sandbox_net` bridge, `EnableICC: false` | Lateral movement between containers |
| Resource limits | `cgroup v2` — `NanoCPUs`, `Memory`, `CpusetCpus` | CPU steal, memory exhaustion |
| Hard timeout | `context.WithTimeout` → SIGTERM → force remove | Hung submissions blocking evaluation slots |

#### Seccomp Profile — Allowlist Design

`seccomp/profile.json` uses `defaultAction: SCMP_ACT_ERRNO` (deny-by-default):

```
Permitted categories:
  File I/O        read, write, open*, stat*, dup*, fcntl, epoll_*, inotify_*
  Networking      socket, bind, connect, listen, accept*, send*, recv*
  Memory          mmap, mprotect, brk, mlock, munmap
  Threading       clone, futex, set_tid_address, arch_prctl, gettid
  Signals         rt_sigaction, rt_sigprocmask, kill
  Time            clock_gettime, nanosleep, timerfd_*
  Process         execve (start only), exit_group, wait4, getpid, getppid

Explicitly denied (default ERRNO):
  mount, umount2, ptrace, kexec_load, init_module, finit_module,
  perf_event_open, process_vm_readv, process_vm_writev, bpf, capset, seccomp
```

### 4.3 Container Manager (`sandbox/internal/manager/manager.go`)

```go
type KafkaConfig struct { Broker, EBPFTopic string }

type Manager struct {
    cli        *client.Client     // Docker SDK v25.0.8
    seccompJSON string
    kafka      KafkaConfig
    mu         sync.RWMutex
    sandboxes  map[string]*Info   // session_id → container info
    ipRegistry map[string]string  // container_id → IP (for eBPF correlation)
}
```

Lifecycle per evaluation session:

```
Spawn(req) {
    ContainerCreate(hostConfig: seccomp + capDrop + cpuset + readonlyRootfs)
    ContainerStart()
    ContainerInspect() → IP on sandbox_net
    ipRegistry[id] = ip
    resolveBridgeIface(sandboxNetName)  // "br-<hash>"
    go runEBPFProber(ctx, iface, sessionID, sandboxID, broker)
    go enforceTimeout(ctx, timeoutS)
}

Stop(id) {
    proberCancel()          // stop eBPF prober goroutine
    ContainerStop(SIGTERM, 2s grace)
    ContainerRemove(force=true)
    delete ipRegistry[id]
}
```

### 4.4 HTTP API

```
POST   /v1/sandbox/run          Launch container, returns endpoint URL + session_id
GET    /v1/sandbox/{id}/status  Poll container state
DELETE /v1/sandbox/{id}         Terminate and remove
GET    /healthz                 {"status":"ok"}
```

---

## 5. eBPF Kernel Latency Prober

This is the platform's most technically differentiated component. While the bot fleet measures **application-layer RTT** (Go `time.Now()` around the HTTP round-trip), the eBPF prober independently measures **kernel-layer RTT** by intercepting packets at the TC (Traffic Control) layer — before and after the network stack, with nanosecond-precision `bpf_ktime_get_ns()` timestamps.

### 5.1 Why eBPF TC Hooks

Traditional latency measurement from user space includes:
- Goroutine scheduling jitter
- Go runtime overhead (channel sends, GC pauses)
- Kernel network stack traversal (not measured)

The eBPF approach eliminates all of these. TC hooks execute in the kernel's softirq context, on the critical path of every packet, with no user-space involvement.

```
                  ┌──────────────────────────────────────────────┐
  Outgoing        │  sandbox_net bridge  (br-<hash>)             │
  SYN ──────────► │  TC EGRESS hook: store ktime_ns in LRU_HASH  │
                  │     key = {saddr, daddr, sport, dport, proto} │
  Incoming        │                                              │
  SYN-ACK ──────► │  TC INGRESS hook: lookup ktime_ns from map   │
                  │     delta = ktime_now - ingress_ts            │
                  │     push latency_event to RINGBUF             │
                  └──────────────────────────────────────────────┘
```

### 5.2 eBPF Program (`sandbox/internal/ebpf/bpf/tc_latency.c`)

```c
// LRU hash: flow_key → ingress ktime_ns (max 65536 concurrent flows)
struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __type(key, struct flow_key);   // {saddr, daddr, sport, dport, proto}
    __type(value, __u64);
    __uint(max_entries, 65536);
} ingress_ts SEC(".maps");

// Ring buffer: kernel → user-space (zero-copy, ordered)
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 256 * 1024); // 256 KB
} latency_events SEC(".maps");

SEC("tc")
int tc_ingress(struct __sk_buff *skb) {
    // store ktime_ns keyed by flow
    __u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&ingress_ts, &key, &ts, BPF_ANY);
    return TC_ACT_OK;
}

SEC("tc")
int tc_egress(struct __sk_buff *skb) {
    __u64 *ingress = bpf_map_lookup_elem(&ingress_ts, &key);
    if (!ingress) return TC_ACT_OK;
    __u64 delta = bpf_ktime_get_ns() - *ingress;
    if (delta > 5000000000ULL) return TC_ACT_OK; // discard >5s (dropped)
    struct latency_event *e = bpf_ringbuf_reserve(&latency_events, sizeof(*e), 0);
    if (e) {
        e->rtt_ns = delta;
        // ... fill src/dst ip, port, session_id
        bpf_ringbuf_submit(e, 0);
    }
    bpf_map_delete_elem(&ingress_ts, &key);
    return TC_ACT_OK;
}
```

### 5.3 Go Loader (`sandbox/internal/ebpf/prober.go`)

```go
//go:build linux

func NewProber(iface, sessionID, sandboxID, broker, topic string) (*Prober, error) {
    // Load compiled BPF bytecode (embedded via //go:embed bpf/tc_latency.o)
    spec, _ := loadTcLatencySpec()
    objs := TcLatencyObjects{}
    spec.LoadAndAssign(&objs, nil)

    // Attach to TC ingress and egress on the sandbox bridge interface
    qdisc := &netlink.GenericQdisc{...}   // clsact qdisc
    netlink.QdiscAdd(qdisc)
    netlink.FilterAdd(tcIngressFilter(objs.TcIngress, iface))
    netlink.FilterAdd(tcEgressFilter(objs.TcEgress, iface))
    ...
}

func (p *Prober) Run(ctx context.Context) error {
    rd, _ := ringbuf.NewReader(p.objs.LatencyEvents)
    for {
        record, _ := rd.Read()
        var event LatencyEvent
        binary.Read(bytes.NewReader(record.RawSample), binary.LittleEndian, &event)
        // Publish to Kafka bench.telemetry.kernel_latency
        p.writer.WriteMessages(ctx, kafka.Message{Value: json.Marshal(event)})
    }
}
```

**Platform build tags** ensure the eBPF code only compiles on Linux:
- `prober.go`: `//go:build linux`
- `prober_stub.go`: `//go:build !linux` — no-op struct, macOS dev works normally
- `make ebpf-build`: cross-compiles `tc_latency.c` → `tc_latency.o` in a Linux Docker container

---

## 6. Bot Fleet

### 6.1 Architecture

The bot fleet is a single Go binary (`botfleet/`) that spawns goroutines, one per simulated trading participant. The fleet uses a **worker pool with context cancellation** — all bots stop cleanly when the evaluation session ends.

```go
type Fleet struct {
    engineURL    string
    sessionID    string
    mix          ArchetypeMix
    metricsCh    chan RawMetric
    replaySeq    atomic.Int64    // monotonically increasing, for replay log
}

type ArchetypeMix struct {
    MarketMakers, MomentumTraders, NoiseTraders,
    InstitutionalSlicers, LatencyArbs int
}
```

All bots publish to **two** Kafka topics:
- `bench.raw_metrics` — per-order latency, fill result, used by Telemetry
- `bench.replay_log` — same events with `replay_seq`, used for deterministic replay

### 6.2 Five Market Microstructure Archetypes

Each archetype models a realistic class of market participant, providing statistical diversity in the order flow that stresses different aspects of the contestant's engine.

#### 6.2.1 Market Maker — Avellaneda-Stoikov Model

The mathematically rigorous market making strategy from Avellaneda & Stoikov (2008). It continuously quotes symmetric bid/ask around a reservation price that adjusts for inventory risk.

```
reservation_price = mid_price - q · γ · σ² · (T - t)
spread = γ · σ² · (T - t) + (2/γ) · ln(1 + γ/κ)

where:
  q   = current inventory (signed, units held)
  γ   = risk aversion parameter (0.1)
  σ   = price volatility (10.0)
  T-t = time remaining in session (normalised)
  κ   = order arrival intensity (1.5)
```

Impact: Tests the engine's ability to handle high-frequency, double-sided quote updates. A correct engine must maintain price-time priority as quotes are updated faster than execution.

#### 6.2.2 Momentum Bot — MA Crossover

Trades directionally based on a short/long moving average crossover:

```
shortMA = EMA(prices, window=5)
longMA  = EMA(prices, window=20)

if shortMA > longMA: BUY  market order (trend following)
if shortMA < longMA: SELL market order
```

Impact: Generates bursts of market orders during trend transitions. Tests throughput under sudden liquidity demand.

#### 6.2.3 Institutional Slicer — TWAP Iceberg

Models a large institutional order being sliced to minimise market impact:

```
parent_order = 500 units
slice_size   = 10 units
interval     = 50–200ms (uniform random, TWAP jitter)
```

Sends 50 child limit orders at the NBBO, waiting between slices. Tests the engine's behaviour under sustained, predictable order arrival with partial fills.

#### 6.2.4 Latency Arbitrageur

Fires market orders at maximum speed with no rate limiter:

```go
for {
    go sendMarketOrder(...)  // no sleep, no backoff
}
```

The aggressor that stresses raw throughput (peak TPS). Also validates that the engine handles order bursts without dropping messages or corrupting state.

#### 6.2.5 Noise Trader

Sends random limit orders at random prices and quantities:

```
price ∈ [midPrice × 0.95, midPrice × 1.05]
qty   ∈ [1, 100]
side  ∈ {BUY, SELL} (50/50)
```

Provides baseline order book depth. Without noise traders the book would be empty during lulls, making other archetypes' fills trivially easy.

### 6.3 Correctness Replay Log

Every order is published to `bench.replay_log` with a monotonically-increasing `replay_seq` (Go `atomic.Int64`):

```json
{
  "replay_seq":   4721,
  "session_id":   "abc-123",
  "order_id":     "ord-9f3a",
  "symbol":       "AAPL",
  "side":         "BUY",
  "price":        150.25,
  "quantity":     10,
  "sent_at_ns":   1716200012345678900,
  "fill_price":   150.25,
  "fill_qty":     10
}
```

The Telemetry service replays this log in `replay_seq` order to verify that the engine's fills are consistent with price-time priority — even if network delivery was out of order.

---

## 7. Telemetry & Validation

### 7.1 Composite Scoring Engine (`telemetry/internal/scorer/composite.go`)

The engine consumes `bench.raw_metrics` in batches (Kafka consumer group `telemetry-scorer`) and computes a four-dimension score:

```
TotalScore = 0.30 × ThroughputScore
           + 0.30 × TailLatencyScore
           + 0.25 × CorrectnessScore
           + 0.15 × ResilienceScore
```

#### Throughput Score (30%)

```
tps = order_count / elapsed_seconds
peak_tps = max TPS observed in any 1-second window

throughputScore = min(100, tps / TARGET_TPS × 100)
  where TARGET_TPS = 10,000 orders/sec
```

#### Tail Latency Score (30%)

Uses the **eBPF kernel RTT p99** (not app-layer RTT) — the hardest measurement for contestants to game:

```
tailLatencyScore = 100 × (1 - log(p99_ns / 1000) / log(100_000_000 / 1000))
  clamped to [0, 100]

Intuition:
  p99 ≤ 1µs   → score = 100
  p99 = 1ms   → score ≈ 50
  p99 ≥ 100ms → score = 0
```

Log-scale normalisation prevents a 1µs engine from scoring only marginally better than a 10µs engine.

#### Correctness Score (25%)

```
correctnessScore = fillAccuracy × priceTimePenalty

fillAccuracy = correctly_filled / total_fills     (0.0–1.0)

priceTimePriority penalty:
  violations = count of fills where a better-priority order was skipped
  priceTimePenalty = max(0, 1 - violations / total_fills)

correctnessScore = fillAccuracy × priceTimePenalty × 100
```

#### Resilience Score (15%)

Measured during the chaos injection phase (see §9):

```
recoveryScore   = max(0, 1 - recovery_time_ms / 5000)
degradationScore = max(0, 1 - degradation_ratio)

resilienceScore = (recoveryScore + degradationScore) / 2 × 100
```

### 7.2 HDR Histogram

Uses `hdrhistogram-go` (port of Gil Tene's HdrHistogram). Configured for the range 1ns–10s with 3 significant decimal digits:

```go
h := hdrhistogram.New(1, 10_000_000_000, 3)
h.RecordValue(rtt_ns)

p50  = h.ValueAtQuantile(50)
p90  = h.ValueAtQuantile(90)
p99  = h.ValueAtQuantile(99)
p999 = h.ValueAtQuantile(99.9)
```

Unlike `time.Duration` averaging or percentile-from-sorted-slice approaches, HDR histograms have O(1) insert and O(1) percentile query, making them safe to use on the hot path without locking.

### 7.3 Price-Time Priority Validator (`telemetry/internal/scorer/validator.go`)

Maintains an in-memory order book mirroring what a correct matching engine should produce:

```go
type Validator struct {
    bids []validatedOrder  // sorted: price desc, arrival_time asc
    asks []validatedOrder  // sorted: price asc, arrival_time asc
}

func (v *Validator) RecordFill(id string, fillPrice float64, fillQty int64) bool {
    // For each fill, check: was there a better-priority order resting
    // in the book that should have matched first?
    // A violation = contestant engine skipped a better order.
}
```

Violations increment `PriceTimeViolations` in the composite score output.

### 7.4 TimescaleDB Writer

All raw metrics and scores are persisted to TimescaleDB hypertables (partitioned by time):

```sql
-- Per-order metrics (app-layer RTT)
INSERT INTO raw_metrics
  (time, session_id, sandbox_id, order_id, archetype,
   app_rtt_ns, kernel_rtt_ns, fill_correct, fill_price, fill_qty, replay_seq)
VALUES (...);

-- Composite scores (one row per scoring interval)
INSERT INTO composite_scores
  (time, session_id, team_name, p50_ns, p90_ns, p99_ns, p999_ns,
   tps, peak_tps, fill_accuracy, price_time_violations,
   recovery_time_ms, degradation_ratio,
   throughput_score, tail_latency_score, correctness_score,
   resilience_score, total_score)
VALUES (...);

-- eBPF kernel RTT events
INSERT INTO kernel_latency
  (time, session_id, sandbox_id, src_ip, dst_ip,
   src_port, dst_port, rtt_ns, ingress_ns, egress_ns)
VALUES (...);
```

Hypertables compress old chunks automatically, keeping long-running competitions queryable without storage bloat.

---

## 8. Real-Time Leaderboard

### 8.1 Go WebSocket Hub (`leaderboard/server/main.go`)

```go
type Hub struct {
    mu      sync.RWMutex
    scores  map[string]*CompositeScore  // session_id → latest score
    clients map[*websocket.Conn]struct{}
}
```

The hub has three invariants:
1. **Write lock** only on `upsert()` and client add/remove — reads are concurrent
2. **Snapshot-on-connect** — a new client immediately receives the current leaderboard, no polling required
3. **Broadcast after every upsert** — all clients see score updates within the Kafka consumer's `CommitInterval` (1 second)

```go
func (h *Hub) broadcast() {
    snap := h.snapshot()  // sorts by TotalScore desc, assigns Rank 1..N
    // Fan out to all clients concurrently; disconnect on write timeout (3s)
}
```

Using `nhooyr.io/websocket` (not `gorilla/websocket`) for its context-aware API — write timeouts are enforced via `context.WithTimeout`, preventing a slow client from blocking the broadcast loop.

### 8.2 Kafka Consumer

The leaderboard server runs a single Kafka consumer in a goroutine:

```go
reader := kafka.NewReader(kafka.ReaderConfig{
    Brokers:     []string{broker},
    Topic:       "bench.scores",
    GroupID:     "leaderboard-ws",
    StartOffset: kafka.LastOffset,  // show only live data, not history
})
```

`StartOffset: LastOffset` is intentional — the leaderboard shows the live competition, not a replay. Historical data is in TimescaleDB for post-hoc analysis.

### 8.3 React Frontend

Single-page app built with Vite + TypeScript. Connects to `ws://<host>/ws/leaderboard` on load:

```typescript
const ws = new WebSocket(`ws://${window.location.host}/ws/leaderboard`);
ws.onmessage = (e) => {
    const snapshot: LeaderboardSnapshot = JSON.parse(e.data);
    setEntries(snapshot.entries);  // React state → re-render
};
```

Displays rank, team name, p50/p99 latency, TPS, fill accuracy, and total score per contestant. Updates in real time as `bench.scores` receives new messages.

---

## 9. Chaos Engineering

### 9.1 Design Philosophy

Resilience under degraded network conditions is a realistic production requirement for trading systems. The chaos injector (`sandbox/internal/chaos/injector.go`) deliberately degrades the sandbox's network and CPU, measures how quickly the contestant's engine recovers, and feeds the result into the resilience dimension of the composite score.

### 9.2 Fault Schedule

```go
func DefaultFaultSchedule() []FaultSpec {
    return []FaultSpec{
        {Type: NetworkDelay, DelayMs: 5, JitterMs: 2, Duration: 10 * time.Second},
        {Type: NetworkLoss,  LossPct: 2,               Duration: 10 * time.Second},
        {Type: CPUThrottle, CPUPct: 50,                Duration: 15 * time.Second},
    }
}
```

### 9.3 Implementation

**Network degradation** — Linux `tc netem` qdisc on the sandbox bridge:

```bash
# Inject 5ms delay + 2ms jitter
tc qdisc add dev <iface> root netem delay 5ms 2ms distribution normal

# Inject 2% packet loss (Gilbert-Elliott model)
tc qdisc add dev <iface> root netem loss gemodel 2%

# Remove fault
tc qdisc del dev <iface> root
```

**CPU throttle** — cgroup v2 `cpu.max`:

```bash
# Limit to 50% of one CPU core
echo "50000 100000" > /sys/fs/cgroup/<sandbox_cgroup>/cpu.max

# Restore full CPU
echo "max 100000" > /sys/fs/cgroup/<sandbox_cgroup>/cpu.max
```

### 9.4 Resilience Measurement

```
baselineP99 = p99 measured before any fault injection

For each fault:
  inject fault
  measure p99 every 500ms for fault.Duration
  degradation_ratio = (max_p99_during_fault - baselineP99) / baselineP99

After fault removed:
  poll p99 every 200ms until p99 ≤ baselineP99 × 1.1
  recovery_time_ms = time from fault removal to recovery
```

An engine that degrades gracefully and recovers quickly scores high on resilience.

---

## 10. Inter-Service Communication

### Redpanda Topics

| Topic | Producer | Consumer(s) | Schema | Retention |
|---|---|---|---|---|
| `bench.raw_metrics` | Bot Fleet | Telemetry | `RawMetric` JSON | 24h |
| `bench.replay_log` | Bot Fleet | Telemetry | `ReplayEvent` JSON (+ `replay_seq`) | 7d |
| `bench.scores` | Telemetry | Leaderboard | `CompositeScore` JSON | 24h |
| `telemetry.kernel_latency` | Sandbox (eBPF prober) | Telemetry | `LatencyEvent` JSON | 24h |

### Protobuf Contracts (`proto/`)

Inter-service gRPC uses `.proto` files in the repo root. Generated stubs live in `gen/proto/`:

```protobuf
// proto/common.proto
message CompositeScore {
  string session_id = 1;  string team_name = 2;
  double p50_ns = 3;      double p90_ns = 4;
  double p99_ns = 5;      double p999_ns = 6;
  double tps = 7;         double peak_tps = 8;
  double fill_accuracy = 9;
  int64  price_time_violations = 10;
  double recovery_time_ms = 11;  double degradation_ratio = 12;
  double throughput_score = 13;  double tail_latency_score = 14;
  double correctness_score = 15; double resilience_score = 16;
  double total_score = 17;       int64  computed_at_ns = 18;
}

enum BotArchetype {
  NOISE_TRADER = 0; MARKET_MAKER = 1; MOMENTUM = 2;
  INSTITUTIONAL_SLICER = 3; LATENCY_ARB = 4;
}
```

---

## 11. Data Stores

### TimescaleDB Schema

Three hypertables, all partitioned by `time` with 1-week chunks:

```sql
-- Raw per-order metrics (app-layer RTT from bot fleet)
raw_metrics (time, session_id, sandbox_id, order_id, archetype,
             app_rtt_ns, kernel_rtt_ns, fill_correct, fill_price, fill_qty, replay_seq)
INDEX: (session_id, time DESC)

-- Composite scores per scoring interval
composite_scores (time, session_id, team_name, p50_ns..p999_ns, tps, peak_tps,
                  fill_accuracy, price_time_violations, recovery_time_ms,
                  degradation_ratio, ...scores, total_score)
INDEX: (session_id, time DESC)

-- eBPF kernel-level RTT events
kernel_latency (time, session_id, sandbox_id, src_ip, dst_ip,
                src_port, dst_port, rtt_ns, ingress_ns, egress_ns)
INDEX: (session_id, time DESC)
```

Useful analytical queries:

```sql
-- Latency percentiles over a session (using TimescaleDB continuous aggregates)
SELECT time_bucket('10 seconds', time) AS bucket,
       percentile_disc(0.50) WITHIN GROUP (ORDER BY kernel_rtt_ns) AS p50,
       percentile_disc(0.99) WITHIN GROUP (ORDER BY kernel_rtt_ns) AS p99
FROM kernel_latency
WHERE session_id = 'abc-123'
GROUP BY bucket ORDER BY bucket;

-- Live leaderboard query (fallback if WebSocket is unavailable)
SELECT DISTINCT ON (session_id) *
FROM composite_scores
ORDER BY session_id, time DESC;
```

### Redis

Used for active session tracking and hot-path leaderboard state:

```
HSET session:<id> endpoint <url> started_at <ts> status running
ZADD leaderboard <total_score> <session_id>
ZREVRANGE leaderboard 0 9 WITHSCORES  -- top 10
```

TTL of 2× evaluation timeout ensures stale sessions are cleaned up automatically.

---

## 12. Infrastructure as Code

### Terraform Modules (`infra/terraform/`)

```
infra/terraform/
├── main.tf           — provider (AWS eu-north-1), remote state (S3)
├── eks.tf            — EKS 1.30 cluster, two node groups:
│                         sandbox-nodes  (c6i.xlarge, 4 vCPU / 8GB, spot)
│                         botfleet-nodes (c6i.2xlarge, 8 vCPU / 16GB, on-demand)
├── networking.tf     — VPC /16, 3 public + 3 private subnets, NAT gateway
├── rds.tf            — RDS Aurora PostgreSQL 16 (TimescaleDB extension)
├── elasticache.tf    — ElastiCache Redis 7
└── variables.tf      — cluster_name, region, node_instance_type
```

### Helm Charts (`infra/helm/`)

```
infra/helm/
├── sandbox/          — Deployment, Service :8080, RBAC (needs Docker socket)
├── botfleet/         — Deployment with HPA (scale on CPU), ConfigMap for archetype mix
├── telemetry/        — Deployment, CronJob for score aggregation
└── leaderboard/      — Deployment, Service :8082, Ingress (ALB)
```

### One-Command Deploy

```bash
cd infra/terraform
terraform init
terraform apply -var="cluster_name=quant-titans-prod"

# After EKS is up:
aws eks update-kubeconfig --name quant-titans-prod --region eu-north-1
helm upgrade --install platform infra/helm/platform/ \
  --set redpanda.brokers=<brokers> \
  --set timescaledb.host=<rds-endpoint>
```

---

## 13. CI/CD Pipeline

### GitHub Actions (`.github/workflows/smoke.yml`)

Every push to `main` or a feature branch triggers a full end-to-end smoke test on `ubuntu-latest`:

```
Job: End-to-end smoke test (passes in ~2.5 minutes)

Steps:
  1. Free disk space (remove dotnet / android SDK — frees ~6GB)
  2. docker compose --profile smoke up --build -d
     - Redpanda v23.3.21
     - TimescaleDB 2.15.2-pg16
     - Redis 7-alpine
     - leaderboard (Go + React, multi-stage build)
     - telemetry (Go)
     - botfleet (Go)
     - sandbox (Go, with eBPF stub)
     - dummy-engine (Go, in-memory matching engine)
  3. Wait for Redpanda cluster health (rpk cluster health)
  4. Wait for TimescaleDB (pg_isready)
  5. Wait for dummy engine /healthz
  6. Wait for leaderboard /healthz
  7. Fire test order → dummy engine POST /v1/order
  8. Assert response contains "status" field
  9. Print /stats (per-symbol book depth)
```

The CI badge on `main` is live evidence that the full pipeline compiles and runs end-to-end on a clean Linux machine.

---

## 14. Composite Scoring Algorithm

Full formula with constants:

```
TotalScore = 0.30 × ThroughputScore(tps, TARGET_TPS=10_000)
           + 0.30 × TailLatencyScore(p99_kernel_ns)
           + 0.25 × CorrectnessScore(fillAccuracy, priceTimeViolations)
           + 0.15 × ResilienceScore(recoveryTimeMs, degradationRatio)

ThroughputScore  = clamp(tps / 10_000 × 100, 0, 100)

TailLatencyScore = clamp(
    100 × (1 - log(p99/1_000) / log(100_000_000/1_000)), 0, 100)
    // 100 at p99≤1µs, ~50 at p99=1ms, 0 at p99≥100ms

CorrectnessScore = fillAccuracy × priceTimePenalty × 100
    fillAccuracy      = correct_fills / total_fills
    priceTimePenalty  = clamp(1 - violations/total_fills, 0, 1)

ResilienceScore  = (recoveryScore + degradationScore) / 2 × 100
    recoveryScore    = clamp(1 - recoveryTimeMs/5_000, 0, 1)
    degradationScore = clamp(1 - degradationRatio, 0, 1)
```

All scores are in \[0, 100\]. A perfect engine (sub-microsecond kernel p99, zero violations, instant chaos recovery) scores 100.

---

## 15. Technology Decisions

| Decision | Choice | Alternative considered | Rationale |
|---|---|---|---|
| Sandbox runtime | Docker + SDK v25 | Kubernetes Jobs | Docker simpler for single-host POC; K8s Jobs for multi-node (IaC ready) |
| Seccomp approach | Allowlist (deny-by-default) | Docker default blocklist | Deny-by-default is the only safe baseline for unknown binaries |
| Kernel latency | eBPF TC hooks (cilium/ebpf-go) | `tcpdump` / `libpcap` | Zero copy, nanosecond precision, no user-space overhead |
| Messaging bus | Redpanda v23.3 | Apache Kafka | Kafka-compatible, 3-5× lower tail latency, single binary |
| Latency histograms | HdrHistogram-go | `time.Since` average | O(1) insert/query; captures tail accurately; averages hide p99 blowout |
| Metrics DB | TimescaleDB 2.15 / pg16 | InfluxDB | Full SQL, hypertable compression, same query tooling as relational data |
| WebSocket library | nhooyr.io/websocket | gorilla/websocket | Context-aware API; write deadlines are enforced by `context.WithTimeout` |
| Bot fleet language | Go goroutines | Rust async / Python | Goroutine-per-bot scales to 1000+ with minimal overhead; no GIL |
| Order protocol | REST + FIX stub | WebSocket only | FIX signals trading domain knowledge; REST for simplicity in smoke tests |
| IaC | Terraform + Helm | AWS CDK / Pulumi | Industry standard; reproducible `plan`/`apply` cycle; judges can read HCL |
| Build system | Makefile | Bazel | Sufficient complexity level; `make smoke`, `make proto`, `make deploy` |

---

## 16. Architecture Decision Records

| ADR | Title | Status |
|---|---|---|
| [ADR-001](adr/001-seccomp-allowlist.md) | Use allowlist seccomp over Docker default blocklist | Accepted |
| ADR-002 | eBPF TC hooks for kernel-layer RTT (not application-layer only) | Accepted |
| ADR-003 | Dual Kafka topics: `raw_metrics` (telemetry) + `replay_log` (correctness) | Accepted |
| ADR-004 | HdrHistogram over sorted-slice percentile for latency measurement | Accepted |
| ADR-005 | `nhooyr.io/websocket` over `gorilla/websocket` for WebSocket server | Accepted |
| ADR-006 | `StartOffset: LastOffset` in leaderboard Kafka consumer | Accepted |

### ADR-002 Summary — eBPF TC Hooks

**Context:** App-layer RTT measurements (Go `time.Now()` around HTTP calls) are polluted by goroutine scheduling jitter, GC pauses, and OS network stack traversal. During chaos injection (CPU throttle), app-layer measurements over-report latency.

**Decision:** Run a parallel eBPF TC hook prober that measures RTT at the kernel packet level, before any user-space involvement. The composite score uses eBPF p99 for the tail latency dimension.

**Consequences:** Requires Linux for the eBPF prober (macOS dev uses a stub). The `make ebpf-build` target cross-compiles the BPF C code in a Docker container on any host.

### ADR-003 Summary — Dual Kafka Topics for Correctness

**Context:** Network delivery order is not guaranteed. A naive validator reading fills in arrival order would produce false violations.

**Decision:** Every order event carries a monotonically-increasing `replay_seq` (Go `atomic.Int64`). The telemetry service replays events in `replay_seq` order before calling the price-time validator, making correctness validation deterministic regardless of network reordering.

### ADR-006 Summary — Leaderboard StartOffset

**Context:** The leaderboard Kafka consumer uses `StartOffset: kafka.LastOffset`. A new leaderboard server instance will not replay historical scores from the beginning of the topic.

**Decision:** Historical data lives in TimescaleDB and is queryable via SQL. The leaderboard WebSocket shows the live competition only. On connect, clients receive the current in-memory snapshot (all sessions scored since the server started), built from the hub's `scores` map.

---

## 17. Performance Characteristics

### Expected Throughput

| Component | Target | Notes |
|---|---|---|
| Bot Fleet | 10,000 orders/sec | 1000 bots × 10 orders/sec each |
| Redpanda | 500,000 msg/sec | Far above our requirements |
| Telemetry ingestion | 10,000 metrics/sec | One goroutine per Kafka partition |
| HDR histogram insert | O(1), ~50ns | No locking on hot path |
| WebSocket broadcast | <1s latency | Kafka CommitInterval = 1s |

### Latency Budget (end-to-end score update)

```
Order sent by bot                         t=0
Order processed by contestant engine      t=0 + engine_latency (measured)
App-layer RTT published to Kafka          t + ~100µs (batching)
eBPF event published to Kafka             t + ~10µs (kernel path)
Telemetry consumes + computes score       t + ~200ms (Kafka poll interval)
Score published to bench.scores           t + ~200ms + scoring_time
Leaderboard WebSocket broadcast           t + ~1s (CommitInterval)
Browser re-render                         t + ~1.01s
```

### Resource Profile (per contestant sandbox)

```
CPU:    2 cores pinned (cpuset), 50% throttle during chaos
Memory: 512 MB hard limit (cgroup v2)
Network: sandbox_net bridge, ICC disabled, rate ~1 Gbps shared
eBPF:  ~256KB ring buffer, ~65536-entry LRU hash map
```

---

*Maintained by Emmanuel Adutwum. This document reflects the Week 3 implementation state (2026-05-20). All described components are deployed and tested via CI.*
