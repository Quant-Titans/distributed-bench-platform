# ADR-007 — TimescaleDB as the Historical Metrics Store

**Status:** Accepted  
**Date:** 2026-05-22  
**Author:** Emmanuel Adutwum

---

## Context

The platform captures high-cardinality time-series data:
- Per-order raw metrics (app-layer RTT, fill correctness) from the bot fleet — up to 50,000 rows/sec during peak throughput
- Composite scores computed by the telemetry service — one row per scoring cycle per session (~1/sec)
- eBPF kernel-level latency events — one row per completed TCP flow

This data must be:
1. Queryable with sub-second response for real-time dashboard queries
2. Compressible for long-running competitions without storage bloat
3. Accessible via standard SQL (judges, analysts, and post-competition review)

Additionally, the leaderboard server needs to expose a `/api/timeseries` HTTP endpoint so the frontend can request historical trend data for a session (e.g., `GET /api/timeseries?session_id=X&metric=p99&limit=100`).

---

## Decision

Use **TimescaleDB** (PostgreSQL extension) as the sole historical metrics store, accessed from the leaderboard server via `pgx/v5` connection pool.

### Schema

Three hypertables, partitioned by `time` with 1-week chunks:

```sql
-- Per-order raw metrics
CREATE TABLE raw_metrics (
    time         TIMESTAMPTZ NOT NULL,
    session_id   TEXT, sandbox_id TEXT, order_id TEXT, archetype TEXT,
    app_rtt_ns   BIGINT, kernel_rtt_ns BIGINT,
    fill_correct BOOLEAN, fill_price FLOAT8, fill_qty BIGINT, replay_seq BIGINT
);
SELECT create_hypertable('raw_metrics', 'time');

-- Composite scores (one row per scoring cycle per session)
CREATE TABLE composite_scores (
    time               TIMESTAMPTZ NOT NULL,
    session_id         TEXT, team_name TEXT,
    p50_ns FLOAT8, p90_ns FLOAT8, p99_ns FLOAT8, p999_ns FLOAT8,
    tps FLOAT8, peak_tps FLOAT8, fill_accuracy FLOAT8,
    price_time_violations BIGINT,
    recovery_time_ms FLOAT8, degradation_ratio FLOAT8,
    throughput_score FLOAT8, tail_latency_score FLOAT8,
    correctness_score FLOAT8, resilience_score FLOAT8, total_score FLOAT8
);
SELECT create_hypertable('composite_scores', 'time');
```

### Write path

The telemetry service writes to TimescaleDB **asynchronously** via a buffered channel in `store.go`. The hot path (Kafka consume → score compute → publish) is not blocked by DB I/O:

```go
func (s *Store) WriteCompositeScore(row CompositeScoreRow) {
    select {
    case s.scoreCh <- row:
    default:
        // Channel full: drop rather than block the scoring loop
    }
}
```

### Read path — `/api/timeseries`

The leaderboard server opens a `pgxpool` on startup (configured via `TIMESCALE_DSN`). The endpoint uses a column allowlist to prevent SQL injection:

```go
allowed := map[string]string{
    "p50": "p50_ns", "p90": "p90_ns", "p99": "p99_ns",
    "p999": "p999_ns", "tps": "tps", "total": "total_score",
}
```

Query example:

```sql
SELECT extract(epoch from time)*1000 AS ts_ms, p99_ns AS value
FROM composite_scores
WHERE session_id = $1
ORDER BY time DESC
LIMIT $2
```

Response:
```json
{
  "session_id": "s-team-alpha-1234",
  "metric": "p99",
  "points": [{"ts_ms": 1716200012345, "value": 45200}]
}
```

---

## Consequences

**Benefits:**
- Full SQL: `time_bucket()`, `percentile_disc()`, `DISTINCT ON` — all standard analytical patterns work without custom code
- Hypertable compression reduces storage 10-20× for older chunks, keeping long-running competitions queryable without disk pressure
- Single data store for both hot writes (via async channel) and cold reads (via pgx pool) — no ETL pipeline
- `pgx/v5` is already a direct dependency (no ORM overhead); queries are parameterised, so SQL injection is not possible

**Trade-offs:**
- `TIMESCALE_DSN` is optional — if unset, `/api/timeseries` returns 503. The leaderboard is fully functional without TimescaleDB (WebSocket sparklines come from the in-memory score history, not the DB).
- Async writes mean the DB view lags the live leaderboard by 1-3 seconds. This is acceptable — the DB is for historical analysis, not the real-time score feed.
- TimescaleDB runs as a StatefulSet in the kind cluster. If the PVC is lost, historical data is gone. For a competition, this is acceptable; a production deployment would add WAL archiving.

---

## Alternatives Considered

| Option | Why rejected |
|---|---|
| InfluxDB | No SQL; Flux query language is unfamiliar; separate storage engine from relational data |
| ClickHouse | Excellent for analytics but requires a separate cluster; full SQL dialect differences |
| Plain PostgreSQL | No hypertable partitioning or compression; long sessions would slow range queries |
| Prometheus / Grafana | Read-only push model; can't query arbitrary session_id ranges; adds two more services |
| Redis time-series | Not persistent across restarts; limited retention; no SQL |
