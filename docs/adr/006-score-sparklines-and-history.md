# ADR-006 — Score Sparklines and In-Memory Score History

**Status:** Accepted  
**Date:** 2026-05-22  
**Author:** Emmanuel Adutwum

---

## Context

The leaderboard shows a single `TotalScore` number per session, updated once per Kafka `CommitInterval` (~1 second). Judges and contestants watching the live leaderboard have no sense of trend: is a team's score improving, oscillating, or plateauing?

Requirements:
- Show a score trend line (sparkline) per team, inline in the leaderboard row
- No additional data store dependency (TimescaleDB is async and has ~1s write lag)
- Must survive WebSocket reconnects (sparkline data must be available on re-connect)

---

## Decision

Track the **last 30 `TotalScore` values** per session inside the telemetry `sessionState` struct. After each Kafka batch is processed, the Engine appends the new score and trims to a rolling window of 30 points:

```go
type sessionState struct {
    // ...existing fields...
    scoreHistory []float64 // last 30 TotalScore values
}

// In Handle(), after Compute():
s.scoreHistory = append(s.scoreHistory, score.TotalScore)
if len(s.scoreHistory) > 30 {
    s.scoreHistory = s.scoreHistory[len(s.scoreHistory)-30:]
}
score.ScoreHistory = make([]float64, len(s.scoreHistory))
copy(score.ScoreHistory, s.scoreHistory)
```

The `score_history` field is included in every `CompositeScore` published to `bench.scores`. The leaderboard server rebroadcasts it to all WebSocket clients as part of every `LeaderboardSnapshot`. The React frontend renders an **inline SVG polyline**:

```tsx
const Sparkline = ({ data, width = 80, height = 28 }) => {
  const pts = data.map((v, i) => {
    const x = (i / (data.length - 1)) * width
    const y = height - ((v - min) / range) * (height - 4) - 2
    return `${x.toFixed(1)},${y.toFixed(1)}`
  }).join(' ')
  return (
    <svg width={width} height={height}>
      <polyline points={pts} fill="none" stroke="#00d4ff" strokeWidth={1.5} />
    </svg>
  )
}
```

30 points × 1 update/sec = 30-second rolling window. At 50-byte average per `CompositeScore` JSON, 30 floats add ~240 bytes per session per Kafka message — negligible.

---

## Consequences

**Benefits:**
- Zero new dependencies. No Redis, no TimescaleDB query on the hot path.
- The sparkline is embedded in the Kafka message and the WebSocket snapshot, so a newly-connecting browser client gets the full trend history immediately without a separate API call.
- 30-point window is enough to show the trajectory during the 120-second benchmark run (one point every ~4 seconds on average at Kafka CommitInterval=1s and batching).

**Trade-offs:**
- History is lost when the telemetry service restarts. On restart it replays `bench.raw_metrics` from where its consumer group left off, not from the beginning. Score history before the restart is gone.
- 30 is an arbitrary cap. Longer windows use proportionally more memory (negligible) and Kafka message space (still negligible), but the SVG polyline becomes visually dense above ~50 points. 30 is a good visual resolution for the 120s benchmark window.

---

## Alternatives Considered

| Option | Why rejected |
|---|---|
| Query TimescaleDB per WebSocket connect | Adds ~50ms latency per connect; requires a DB round-trip on the critical path |
| Redis sorted set for history | Adds operational dependency; overkill for 30 float values |
| Full history (no cap) | Unbounded memory; Kafka message size grows linearly with session duration |
| Chart.js or D3 chart component | Too heavy for an inline trend indicator; SVG polyline is <30 lines of code |
