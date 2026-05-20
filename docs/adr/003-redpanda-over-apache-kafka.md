# ADR-003: Use Redpanda Instead of Apache Kafka

**Status:** Accepted  
**Date:** 2026-05-17  
**Author:** Emmanuel Adutwum

## Context

The platform needs a durable, ordered message bus for three high-throughput streams:

| Stream | Volume estimate (1000 bots, 100 s run) |
|---|---|
| `bench.raw_metrics` | ~500 000 messages (one per order) |
| `bench.replay_log` | ~500 000 messages (replay copy of every order) |
| `telemetry.kernel_latency` | ~200 000 messages (one per eBPF RTT event) |

Options:
- **Apache Kafka + Zookeeper/KRaft:** Industry standard. Requires separate ZooKeeper ensemble (or KRaft quorum), JVM, significant tuning.
- **Redpanda:** Kafka-compatible API, written in C++, single binary, no ZooKeeper. Built-in admin REST API and schema registry.
- **NATS JetStream:** Lower throughput ceiling; not Kafka-wire-compatible, so any consumer code change would be needed if judges compare against Kafka.

## Decision

Use **Redpanda** in single-binary mode. The Kafka-wire-protocol compatibility means `segmentio/kafka-go` works unchanged — the producer and consumer code does not know it is talking to Redpanda. For the hackathon infrastructure, a single-node Redpanda instance is sufficient; upgrading to a 3-replica cluster for production is a `statefulset.replicas: 3` change in values.yaml.

## Consequences

**Good:**
- Single Docker image, no ZooKeeper sidecar — `docker-compose up` starts the entire broker in one container.
- Redpanda's C++ implementation has lower tail latency than JVM Kafka at the message sizes we use (< 1 KiB).
- Includes `rpk` CLI for topic inspection during development and demos.
- No code changes required if judges want to swap in Kafka — consumers use the standard Kafka protocol.

**Bad:**
- Redpanda is not Apache Kafka. If the judging rubric specifically requires Kafka, a 5-line change to the docker-compose image name is needed.
- Single-node loses all durability guarantees on node failure; acceptable for a hackathon benchmark run (each run is ephemeral anyway).
- The Redpanda Helm chart schema (console sub-chart) requires explicit null-guarding for `console.ingress.className` and `console.service.targetPort` — discovered during CI.
