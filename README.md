# Distributed Benchmarking & Hosting Platform

**Team:** Quant Titans | **Competition:** IICPC Summer Hackathon 2026
**Timeline:** May 9 – June 10, 2026

## What We're Building

A platform that evaluates contestant-submitted trading infrastructure
(order books, matching engines) by:

1. Sandboxing contestant code in isolated containers (seccomp, CPU pinning)
2. Spawning 1000+ distributed trading bots that bombard the submission
3. Measuring p50/p90/p99 latency, max TPS, and correctness (price-time priority)
4. Streaming live results to a real-time leaderboard

## Architecture

```
Submission & Sandboxing  ──deploy──►  Distributed Bot Fleet
         │                                      │
    endpoint URL                      orders (FIX/WS) + metrics (Kafka)
         │                                      │
         ▼                                      ▼
  Telemetry & Validation  ──scores──►  Real-Time Leaderboard
```

## Tech Stack

| Layer | Choice |
|---|---|
| Sandbox | Docker + Kubernetes (seccomp, namespaces, CPU pinning) |
| Bot Fleet | Go (goroutines) + FIX/WebSocket |
| Comms | gRPC + Protocol Buffers |
| Messaging | Redpanda (Kafka-compatible) |
| Metrics DB | TimescaleDB + Redis |
| IaC | Terraform + Helm |
| Frontend | React + WebSocket |
| Cloud | AWS EKS (eu-north-1) |

## Quick Start

```bash
# Local dev (all services)
docker-compose up

# Run tests
make test

# Regenerate gRPC stubs
make proto

# Lint all services
make lint

# Deploy to AWS (Week 3+)
cd infra/terraform && terraform apply
```

## Repository Structure

```
distributed-bench-platform/
├── sandbox/           # Docker/K8s sandboxing engine
├── botfleet/          # Go distributed load generator
│   ├── cmd/
│   └── internal/
│       ├── fix/       # FIX protocol order generation
│       ├── ws/        # WebSocket order generation
│       └── worker/    # Bot worker pool
├── telemetry/         # Ingester, TimescaleDB, Redis, validator
├── leaderboard/       # React + WebSocket frontend
├── infra/
│   ├── terraform/     # AWS EKS, VPC, security groups
│   └── helm/          # Helm charts per service
├── proto/             # Shared gRPC protobuf definitions
└── docs/              # Architecture blueprint + ADRs
```

## Team

| Member | Role |
|---|---|
| Emmanuel Adutwum | Infrastructure Architect — sandbox, IaC, AWS, telemetry pipeline |
| Shiv Kumar Mishra | Distributed Systems Lead — bot fleet, gRPC, Kafka, latency, correctness |
| Pathan Farhana | Frontend & Scoring Lead — React leaderboard, WebSocket, composite scoring |

## Branch Strategy

```
main                ← protected; requires PR + review
feat/sandbox-*      ← Emmanuel
feat/botfleet-*     ← Shiv Kumar
feat/leaderboard-*  ← Farhana
feat/infra-*        ← Emmanuel
fix/*               ← any member
docs/*              ← any member
```
