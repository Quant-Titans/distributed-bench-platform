# End-to-End Demo Script — Quant Titans

**Competition:** IICPC Summer Hackathon 2026  
**Duration:** ~8 minutes live, ~2 min for judges to observe leaderboard updating

---

## Pre-flight (do this before presenting)

```bash
# Tear down any leftover state
make down

# Confirm Docker has 4+ GB free
docker system df
```

---

## Live Demo Sequence

### 1 — One-command platform start (30 seconds)

```bash
make demo
```

What judges see:
- Docker builds 4 images in parallel
- Services start sequentially via health-check dependencies
- CLI confirms each service healthy:
  ```
    ✓ sandbox  :8080
    ✓ botfleet :9091
    ✓ leaderboard :8082
  ```

**Talking point:** "Entire platform from `git clone` to live in under 60 seconds. Same command deploys to AWS EKS via Terraform — one command, no manual steps."

---

### 2 — First binary upload (20 seconds)

The `make demo` command automatically:
1. Builds the dummy matching engine binary (~2 MB Go binary)
2. POSTs it to `POST /v1/upload` with team name "Team Alpha"
3. Sandbox wraps it in a Docker image (alpine + libstdc++), starts the container
4. Returns JSON with sandbox ID, endpoint, build time

Expected output:
```json
{
  "id": "a3f9c1d2b4e6",
  "session_id": "demo-alpha",
  "team_name": "Team Alpha",
  "endpoint": "http://172.20.0.5:8080",
  "status": "running",
  "build_ms": 4200,
  "image_tag": "contestant/demo-alpha:latest"
}
```

**Talking point:** "Any compiled binary — Go, C++, Rust. No source code required. We build the container, not them."

---

### 3 — Bot fleet auto-launches (3 seconds after upload)

Behind the scenes, sandbox POSTs to `http://botfleet:9091/v1/spawn`:
```json
{
  "session_id": "demo-alpha",
  "endpoint_url": "http://172.20.0.5:8080",
  "bot_count": 200,
  "target_tps": 1000,
  "duration_secs": 90
}
```

200 goroutines across 5 archetypes start firing orders:
- Market Makers (20%) — Avellaneda-Stoikov continuous quotes
- Momentum Traders (20%) — trend-following
- Noise Traders (30%) — random Poisson
- Institutional Slicers (15%) — TWAP/VWAP
- Latency Arbs (15%) — cancel-and-replace at microsecond precision

**Talking point:** "Not just random noise. Market Makers use Avellaneda-Stoikov optimal quoting. Latency arbs create realistic cancel storms."

---

### 4 — Open leaderboard in browser

```
http://localhost:8082
```

Within ~5 seconds of upload:
- Team Alpha appears on the leaderboard
- Scores update every 500ms via WebSocket push
- Columns visible: **p50 / p90 / p99** latency | **TPS** | **Fill Accuracy** | **Composite Score**

When Team Beta upload completes (runs in same `make demo` sequence):
- Two rows, live ranking reorders as scores change

**Talking point:** "p99 latency from an eBPF TC hook on the bridge network interface — kernel-level timing, not application-layer. No userspace coordination needed. Leaderboard is WebSocket, sub-1-second update."

---

### 5 — Show composite scoring formula (15 seconds)

Open `docs/architecture.md` Section 14 (Scoring) or talk through:

```
Composite = 0.35 × throughput_score
           + 0.35 × tail_latency_score
           + 0.20 × correctness_score
           + 0.10 × resilience_score
```

Where:
- `throughput_score = min(tps / 5000, 1.0)`
- `tail_latency_score = 1 / (1 + p99_ns / 1_000_000)` (sigmoid, favours sub-millisecond)
- `correctness_score = fill_accuracy × (1 − price_time_violations / total_fills)`
- `resilience_score = 1 − degradation_ratio` (how well it holds under chaos injection)

---

### 6 — Verify security isolation (optional, ~1 min)

```bash
# Show the seccomp profile
cat sandbox/seccomp/profile.json | python3 -m json.tool | head -20

# Show a running sandbox container's security opts
docker inspect $(docker ps --filter label=quant-titans.role=sandbox -q | head -1) \
  | python3 -c "import json,sys; c=json.load(sys.stdin)[0]; print(c['HostConfig']['SecurityOpt'])"
```

Expected: `["no-new-privileges:true", "seccomp=..."]`

**Talking point:** "Seccomp allowlist — deny-by-default, every syscall not on the list gets ERRNO. ReadonlyRootfs. CapDrop ALL except NET_BIND_SERVICE. Dedicated bridge network with ICC disabled — containers can't reach each other laterally."

---

### 7 — Deterministic replay (optional, ~30 seconds)

```bash
# Every order is logged to bench.replay_log with monotonic sequence number.
# Any past session can be replayed against a new submission:
docker compose exec redpanda rpk topic consume bench.replay_log --num 5
```

**Talking point:** "Every order in every session is written to a replay log. Judges can replay any historical benchmark against a new submission — completely deterministic, same order sequence, same arrival times."

---

## Cleanup

```bash
make down
# or full teardown:
docker compose down --volumes --remove-orphans
```

---

## Fallback: Manual Flow (if make demo fails)

```bash
# 1. Start platform
docker compose up --build -d

# 2. Wait for health
until curl -sf http://localhost:8080/healthz; do sleep 2; done
until curl -sf http://localhost:9091/healthz; do sleep 2; done

# 3. Build binary
cd dummy-engine && go build -o /tmp/engine . && cd ..

# 4. Upload
curl -X POST http://localhost:8080/v1/upload \
  -F "team_name=Team Alpha" \
  -F "session_id=demo-alpha" \
  -F "binary=@/tmp/engine" \
  -F "timeout_s=90"

# 5. Open http://localhost:8082
```

---

## Key Numbers to Quote

| Metric | Value |
|---|---|
| Bot goroutines per fleet | 200 (scalable to 1000+) |
| Latency measurement | Kernel-level eBPF TC hook, nanosecond precision |
| Leaderboard update frequency | < 1 second via WebSocket |
| Upload → bots firing | < 5 seconds total |
| Seccomp profile | 42 allowlisted syscalls, deny-by-default |
| One-command deploy | `make deploy` (or `terraform apply`) |
| Replay capability | Full deterministic replay from Redpanda |
