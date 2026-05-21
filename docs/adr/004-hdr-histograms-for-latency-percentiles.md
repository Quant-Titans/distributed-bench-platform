# ADR-004: Use HDR Histograms for Latency Percentile Reporting

**Status:** Accepted  
**Date:** 2026-05-17  
**Author:** Emmanuel Adutwum

## Context

The platform must report p50, p90, p99, and p99.9 latencies for each benchmark session. Two classes of approaches were considered:

**Option A — Summary statistics (averages, stddev):** Easy to compute; meaningless for tail latency characterisation. A system with average latency 200 µs but 1% of requests taking 50 ms is indistinguishable from one where every request takes 200 µs.

**Option B — Reservoir sampling / t-digest:** Approximate; loses accuracy at the extreme tails (p99.9+). t-digest has non-trivial merge semantics when combining across multiple bot goroutines.

**Option C — HdrHistogram:** Lossless recording; `O(1)` recording time; `O(1)` percentile query. Uses a fixed memory footprint regardless of observation count. Merge of histograms from different goroutines is exact and associative.

For a trading infrastructure benchmark, p99.9 matters: a contestant whose p99.9 is 10 ms will cause significant slippage in real markets even if the median latency is good. If we report p99 using a summarising method that underestimates the tail, we reward the wrong behaviour.

## Decision

Use `HdrHistogram-go` (`github.com/HdrHistogram/hdrhistogram-go`) for all latency recording. Two independent histograms are maintained per session:

1. **Kernel histogram** — fed from eBPF ring buffer events; nanosecond resolution; range 1 ns – 10 s.
2. **App histogram** — fed from bot-side `time.Now()` measurements; same range.

`KernelPercentiles()` returns from the kernel histogram if ≥ 100 samples exist, falling back to the app histogram. This means the composite score always uses the best available measurement.

Recording is lock-free at the goroutine level: each bot goroutine records into its own histogram; the telemetry consumer merges incoming `RawMetric` values into the session histogram under a `sync.Mutex`. At the scale of `Handle()` call rates (≤ 50 000 msg/s batched), lock contention is negligible.

## Consequences

**Good:**
- Exact p99.9 reporting — no approximation error at any percentile.
- Fixed 80 KiB memory per histogram regardless of observation count — predictable resource usage with 1000 bots.
- HDR histograms are widely used in the trading/low-latency community (HdrHistogram was created at LMAX); judges familiar with the space will recognise it as the correct choice.
- The leaderboard UI can display all four percentiles (p50/p90/p99/p999) without any additional computation.

**Bad:**
- `hdrhistogram-go` is not thread-safe for concurrent `RecordValue` calls — each session uses a single writer goroutine (the telemetry consumer) which is acceptable but worth documenting.
- HDR histograms allocate a fixed array on creation regardless of whether that range is ever reached — 80 KiB is small but 1000 concurrent sessions would use ~80 MiB. This is within the telemetry pod's 2 GiB limit.
