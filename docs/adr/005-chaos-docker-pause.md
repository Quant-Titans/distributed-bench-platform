# ADR-005 — Chaos Fault Injection via Docker Pause/Unpause

**Status:** Accepted  
**Date:** 2026-05-22  
**Author:** Emmanuel Adutwum

---

## Context

The platform needs a chaos fault mechanism that:

1. Works identically on macOS (local dev), Linux CI, and Kubernetes
2. Requires no elevated host privileges beyond the Docker daemon socket
3. Provides a hard, measurable fault window so the telemetry service can compute a precise `recovery_time_ms`
4. Emits structured events to Kafka so the leaderboard can display a real-time ⚡ chaos marker

The original design used **Linux `tc netem`** qdisc commands on the sandbox bridge interface to inject network delay and packet loss. This required:
- Resolving the Docker bridge name (`br-<hash>`) from inside the sandbox container
- `NET_ADMIN` capability on the sandbox host process
- Linux-only `netlink` syscalls — non-functional on macOS

During testing, `resolveBridgeIface()` reliably returned an empty string on macOS Docker Desktop, so `ChaosEnabled` was effectively ignored in the development environment. The feature also failed in GitHub Actions CI because the default Docker network driver does not expose a bridge interface the way bare-metal Linux does.

---

## Decision

Replace `tc netem` chaos with **Docker `ContainerPause` / `ContainerUnpause`** via the Docker SDK.

### Fault schedule

```
t=0s   baseline window begins (p99 recorded as baselineP99NS)
t=30s  ContainerPause(containerID)
       → publishes chaos_start to bench.events
t=35s  ContainerUnpause(containerID)
       → publishes chaos_end to bench.events
       → telemetry records chaosP99NS, computes degradationRatio + recoveryTimeMS
```

### Implementation

```go
func (m *Manager) runChaos(ctx context.Context, info *Info, containerID string) {
    select {
    case <-ctx.Done():
        return
    case <-time.After(30 * time.Second): // baseline collection window
    }
    m.publishChaosEvent(ctx, "chaos_start", "container_pause")
    m.cli.ContainerPause(ctx, containerID)

    select {
    case <-ctx.Done():
        m.cli.ContainerUnpause(context.Background(), containerID)
        return
    case <-time.After(5 * time.Second): // 5-second pause
    }
    m.cli.ContainerUnpause(ctx, containerID)
    m.publishChaosEvent(ctx, "chaos_end", "container_pause")
}
```

`ContainerPause` sends `SIGSTOP` to all processes in the container's cgroup, freezing them completely. From the bot fleet's perspective, the contestant engine stops responding: connection timeouts accumulate, TPS drops to zero, and p99 spikes. After `ContainerUnpause`, the engine resumes exactly where it left off.

---

## Consequences

**Benefits:**
- Works on macOS, Linux bare-metal, and Docker-in-Docker (CI). No `NET_ADMIN` or `SYS_ADMIN` capability required beyond the Docker socket already mounted.
- Zero false negatives: the pause is absolute (SIGSTOP) — not a probabilistic network fault that might let some packets through, making the `degradation_ratio` deterministic.
- `chaos_start` / `chaos_end` events published to `bench.events` let the leaderboard mark the ⚡ fault window in real time and let the telemetry service capture the exact baseline vs chaos p99 without guessing timing.

**Trade-offs:**
- A full process freeze is cruder than network delay simulation. A real trading system would see delayed packets, not a complete halt. The `degradation_ratio` captures recovery speed, not graceful degradation.
- `tc netem`-style faults (delay, jitter, loss) are more realistic for network partition scenarios. These remain the right choice for dedicated Linux infrastructure; Docker pause is the pragmatic cross-platform choice.

---

## Alternatives Considered

| Option | Why rejected |
|---|---|
| `tc netem` on bridge iface | Requires resolving bridge name (fails on macOS + CI), needs `NET_ADMIN`, Linux-only |
| Killing the container (SIGKILL) | Irreversible — the container would need to be restarted, complicating the session lifecycle |
| CPU quota cgroup throttle | Requires writing to `/sys/fs/cgroup` inside the Docker-in-Docker environment — requires `SYS_ADMIN` |
| Iptables `DROP` rules | Same `NET_ADMIN` requirement as netem; iptables not available in all Docker runtimes |
