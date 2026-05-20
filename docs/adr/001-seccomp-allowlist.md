# ADR-001: Use Allowlist Seccomp Profile over Docker Default

**Status:** Accepted  
**Date:** 2026-05-13  
**Author:** Emmanuel Adutwum

## Context

The sandbox must run arbitrary, untrusted contestant binaries (C++, Go, Rust). We need a syscall policy that prevents container escape, privilege escalation, and host damage while still allowing a trading engine to operate (network I/O, threading, memory allocation, timing).

Docker ships a default seccomp profile that blocks ~44 dangerous syscalls but allows ~300+. This blocklist approach assumes submitted code is broadly trustworthy, which is not our threat model.

## Decision

Use an **allowlist profile** (`defaultAction: SCMP_ACT_ERRNO`) that explicitly permits only the syscall categories a trading engine needs and silently denies everything else.

## Consequences

**Good:**
- New dangerous syscalls added to the kernel in the future are denied by default — no profile update needed.
- Attack surface is measurable: the profile is the complete set of permitted kernel interactions.
- Demonstrates security depth to judges; a blocklist profile would not.

**Bad:**
- May break contestant submissions that use unusual syscalls (e.g., `io_uring`, `userfaultfd`). We will add these to the allowlist on request with justification.
- Requires ongoing maintenance as Go/Rust runtimes evolve (e.g., `clone3` was added for newer thread APIs).

## Alternatives Considered

- **Docker default profile** — too permissive for untrusted code; rejected.
- **No seccomp** — unacceptable; single misconfigured submission could affect host.
- **AppArmor only** — path-based, not syscall-level; complementary but not sufficient alone.
