# ADR-002: Use eBPF TC Hooks for Kernel-Level RTT Measurement

**Status:** Accepted  
**Date:** 2026-05-17  
**Author:** Emmanuel Adutwum

## Context

The platform must measure the latency of each order round-trip (send → acknowledgement) for every contestant submission. Two approaches were considered:

**Option A — Application-layer timing:** Record `time.Now()` before sending the HTTP/FIX request and again after the response body is read. Simple to implement; already present in the bot goroutines.

**Option B — Kernel-level eBPF TC hooks:** Attach a BPF program as a TC (Traffic Control) classifier on the `sandbox_net` bridge interface. Record timestamps when packets enter/leave the bridge at the kernel level. Compute RTT inside the BPF program using a `BPF_MAP_TYPE_LRU_HASH` keyed by `(src_ip, dst_ip, src_port)`.

Application-layer timing introduces two sources of noise that cannot be removed:
1. **Go scheduler jitter:** A goroutine calling `time.Now()` may be descheduled by the runtime between the call and the actual syscall firing. Under contention from 1000 concurrent goroutines this adds hundreds of microseconds of bias.
2. **HTTP framing overhead:** HTTP/1.1 request serialisation, TLS (if any), and response parsing are included in the app-layer RTT. These are legitimate platform costs, not contestant latency.

For a benchmarking platform, the score must reflect *the contestant engine's actual processing time*, not incidental benchmark harness overhead.

## Decision

Use **eBPF TC ingress/egress classifiers** as the primary latency measurement path. The BPF program runs in kernel context before any user-space scheduling, so it captures the true network RTT on the bridge interface with nanosecond resolution.

Key implementation choices:
- **TC hook over XDP:** XDP runs in the NIC driver before the network stack. The sandbox uses a virtual bridge (`sandbox_net`), which has no NIC driver. TC hooks run after the bridge has switched the packet, making them compatible with virtual interfaces.
- **`BPF_MAP_TYPE_LRU_HASH`** for in-flight flow state: automatic eviction prevents unbounded memory growth when bots crash without completing their request.
- **`BPF_MAP_TYPE_RINGBUF`** for event delivery: lower overhead than perf event arrays; the Go consumer blocks in `ring.Read()` with zero CPU when idle.
- **Fallback:** `KernelPercentiles()` returns app-layer p99 if fewer than 100 eBPF samples exist (e.g. before the prober attaches), so the system degrades gracefully.

## Consequences

**Good:**
- p99 latency in the scoring formula reflects only the contestant engine's processing, not benchmark overhead.
- The same eBPF hook provides per-flow data for the chaos resilience score (latency before vs. during `tc netem` faults).
- Differentiates the platform from simpler solutions that only measure app-layer RTT.

**Bad:**
- eBPF TC attachment requires `NET_ADMIN` + `SYS_ADMIN` capabilities in the sandbox pod. These are granted only to the sandbox container, not to contestant containers.
- Building the eBPF C object (`tc_latency.o`) requires Linux kernel headers and a cross-compile step (`make ebpf-build`) — it cannot be compiled on macOS.
- Adds `cilium/ebpf-go` as a dependency; the loader uses a build tag (`//go:build linux`) so the service compiles on macOS with a no-op stub.
