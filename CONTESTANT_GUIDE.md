# Contestant Guide — Quant Titans Distributed Bench Platform

This document describes everything your trading engine binary must implement to be evaluated by the platform.

---

## Quick Start

```bash
# Build your engine for Linux/amd64 (required — the sandbox runs on Linux)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o engine_linux .
# — or —
cargo build --release --target x86_64-unknown-linux-musl
# — or —
g++ -O2 -static -o engine_linux engine.cpp

# Submit
curl -X POST http://platform:8080/v1/upload \
  -F "team_name=YourTeam" \
  -F "timeout=120" \
  -F "binary=@./engine_linux"
```

Your binary is wrapped in an Alpine Linux container and started with `ENTRYPOINT ["/engine"]`. It has 120 seconds to process as many orders as possible.

---

## Required HTTP API (port 8080)

The bot fleet sends orders via HTTP. Your engine **must** listen on `:8080`.

### `POST /v1/order` — Submit an order

**Request body** (JSON):

```json
{
  "order_id":  "ord-a1b2c3",
  "symbol":    "AAPL",
  "side":      "BUY",
  "type":      "LIMIT",
  "price":     150.25,
  "quantity":  10
}
```

| Field | Type | Values |
|---|---|---|
| `order_id` | string | Unique per request; echo it back in the response |
| `symbol` | string | Always `"AAPL"` in the default benchmark run |
| `side` | string | `"BUY"` or `"SELL"` |
| `type` | string | `"LIMIT"` or `"MARKET"` |
| `price` | float64 | Limit price; `0` for market orders |
| `quantity` | int | Number of shares |

**Response body** (JSON, HTTP 200):

```json
{
  "order_id":   "ord-a1b2c3",
  "fill_price": 150.25,
  "fill_qty":   10,
  "status":     "FILLED"
}
```

| Field | Type | Values |
|---|---|---|
| `order_id` | string | Echo the request `order_id` |
| `fill_price` | float64 | Execution price; `0` if the order rests on the book |
| `fill_qty` | int | Shares filled; `0` if the order rests on the book |
| `status` | string | `"FILLED"`, `"PARTIAL"`, or `"PENDING"` |

**Price-time priority:** The platform validator checks that your fills respect price-time priority. A higher-priced buy order must always match before a lower-priced one. Within the same price level, earlier arrivals match first.

### `GET /healthz` — Liveness check

Must return HTTP 200 with any JSON body. Recommended:

```json
{"status": "ok"}
```

The platform waits up to 15 seconds for this endpoint to become healthy after container start.

### `GET /stats` — Throughput and book stats (optional)

If present, return throughput metrics and book depth:

```json
{
  "AAPL": {"bids": 42, "asks": 17}
}
```

---

## Optional FIX 4.4 API (port 8443)

Implementing FIX 4.4 is optional but **strongly recommended** — it signals deep trading domain knowledge and improves your Correctness score. Approximately 5% of the bot fleet uses FIX exclusively.

Your engine should listen for TCP connections on `:8443` and implement a minimal FIX session:

### Session flow

```
Bot               Your Engine
 │                     │
 │── Logon (35=A) ────►│
 │◄─ Logon (35=A) ─────│  (echo back)
 │                     │
 │── NOS (35=D) ───────►│  New Order Single
 │◄─ ExecReport (35=8) ─│  Execution Report
 │                     │
 │── Logout (35=5) ────►│
 │◄─ Logout (35=5) ─────│
```

### New Order Single (35=D) — required tags

| Tag | Name | Example |
|---|---|---|
| 11 | ClOrdID | `ord-a1b2c3` |
| 55 | Symbol | `AAPL` |
| 54 | Side | `1`=Buy, `2`=Sell |
| 40 | OrdType | `1`=Market, `2`=Limit |
| 38 | OrderQty | `10` |
| 44 | Price | `150.2500` (omitted for market orders) |
| 60 | TransactTime | `20260522-14:30:00.000` |

### Execution Report (35=8) — required tags in response

| Tag | Name | Notes |
|---|---|---|
| 11 | ClOrdID | Echo from NOS |
| 37 | OrderID | Your server-assigned order ID |
| 17 | ExecID | Unique execution ID |
| 150 | ExecType | `F`=Trade (filled), `0`=New (resting) |
| 39 | OrdStatus | `2`=Filled, `0`=New, `1`=Partial |
| 38 | OrderQty | Echo from NOS |
| 14 | CumQty | Shares filled so far |
| 151 | LeavesQty | Remaining quantity |
| 6 | AvgPx | Average fill price |

### Heartbeat (35=0)

Respond to heartbeats with an empty heartbeat (or echo `TestReqID` tag 112 if present).

---

## Sandbox Security Constraints

Your binary runs inside a locked-down container. These are hard constraints — they cannot be changed.

| Constraint | Detail |
|---|---|
| **Read-only filesystem** | `/` is read-only. Write to `/tmp` (tmpfs overlay) only. |
| **No network egress** | Inter-container communication is disabled (`EnableICC: false`). You cannot reach the internet or other contestant containers. |
| **Seccomp allowlist** | Only a minimal set of syscalls is permitted. Explicitly blocked: `mount`, `ptrace`, `kexec_load`, `init_module`, `perf_event_open`, `bpf`, `capset`, `seccomp`. |
| **Dropped capabilities** | `CapDrop: ALL`. Only `NET_BIND_SERVICE` is restored (so you can bind port 8080 and 8443). |
| **CPU pinning** | 2 CPU cores (default), pinned via `cpuset`. |
| **Memory limit** | 512 MiB hard limit (cgroup v2). |
| **Timeout** | 120 seconds (configurable via `timeout` form field, max 600). |

### Tips

- Do not use `mmap` with `MAP_FIXED` at address 0 — this is blocked.
- Avoid spawning subprocesses via `execve` after startup.
- `/proc` and `/sys` are read-only.
- `SO_REUSEPORT` is permitted (useful for multi-threaded TCP accept loops).

---

## Scoring

```
TotalScore = 0.30 × ThroughputScore
           + 0.30 × TailLatencyScore
           + 0.25 × CorrectnessScore
           + 0.15 × ResilienceScore
```

| Dimension | What's measured | Ceiling |
|---|---|---|
| **Throughput** | Orders/sec sustained over the session | 50,000 TPS = 100 pts |
| **Tail Latency** | Kernel-level p99 RTT (eBPF TC hooks, not app-layer) | ≤1µs = 100 pts, ≥100ms = 0 pts |
| **Correctness** | Fill accuracy × (1 − violation penalty) | 100 pts for zero violations |
| **Resilience** | Recovery speed after a 5-second container pause at t=30s | 100 pts for instant recovery |

The tail latency score uses a **log scale**:

```
score = 100 × (1 − log10(p99_ns / 1_000) / log10(100_000_000 / 1_000))

p99 ≤ 1µs (1,000 ns)    → 100 pts
p99 = 1ms (1,000,000 ns) → ~50 pts
p99 ≥ 100ms             → 0 pts
```

### What "correctness" checks

The validator maintains a shadow order book and checks that every fill respects price-time priority:
- A BUY order at $150.30 must fill before a BUY at $150.25 (price priority)
- Among two BUYs at $150.25, the earlier-arriving one must fill first (time priority)

Each violation reduces the correctness score proportionally (max −50%).

---

## Leaderboard

Connect to `http://platform:8082` to watch your score update in real time. Each row shows:
- Live bot count and sparkline of score history
- p99 kernel-level latency, TPS, fill accuracy
- Detailed score breakdown (click to expand)
- ⚡ indicator during the chaos fault window (t=30–35s)

---

## Example: Minimal Go Engine

```go
package main

import (
    "encoding/json"
    "fmt"
    "net/http"
    "os"
    "sync"
)

type OrderRequest struct {
    OrderID  string  `json:"order_id"`
    Symbol   string  `json:"symbol"`
    Side     string  `json:"side"`
    Type     string  `json:"type"`
    Price    float64 `json:"price"`
    Quantity int64   `json:"quantity"`
}

type OrderResponse struct {
    OrderID   string  `json:"order_id"`
    FillPrice float64 `json:"fill_price"`
    FillQty   int64   `json:"fill_qty"`
    Status    string  `json:"status"`
}

var mu sync.Mutex

func main() {
    http.HandleFunc("/v1/order", func(w http.ResponseWriter, r *http.Request) {
        var req OrderRequest
        json.NewDecoder(r.Body).Decode(&req)

        mu.Lock()
        // TODO: implement your matching engine here
        mu.Unlock()

        json.NewEncoder(w).Encode(OrderResponse{
            OrderID:   req.OrderID,
            FillPrice: req.Price,
            FillQty:   req.Quantity,
            Status:    "FILLED",
        })
    })
    http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
        fmt.Fprint(w, `{"status":"ok"}`)
    })
    addr := os.Getenv("LISTEN_ADDR")
    if addr == "" { addr = ":8080" }
    http.ListenAndServe(addr, nil)
}
```

Build for Linux: `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o engine_linux .`

---

## Submission Form Fields

| Field | Required | Default | Notes |
|---|---|---|---|
| `team_name` | Yes | — | Displayed on leaderboard |
| `binary` | Yes | — | Linux ELF binary (max 128 MiB) |
| `session_id` | No | auto-generated | Must be URL-safe if provided |
| `timeout` | No | `120` | Benchmark duration in seconds (max 600) |
| `cpu_cores` | No | `0,1` | CPU core pinning (cpuset notation) |
| `memory_mb` | No | `512` | Container memory limit in MiB |

---

*Platform maintained by Quant Titans — IICPC Summer Hackathon 2026.*
