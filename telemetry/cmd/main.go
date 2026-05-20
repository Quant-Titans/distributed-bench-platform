package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Quant-Titans/distributed-bench-platform/telemetry/internal/consumer"
	"github.com/Quant-Titans/distributed-bench-platform/telemetry/internal/scorer"
	"github.com/Quant-Titans/distributed-bench-platform/telemetry/internal/store"
)

func main() {
	broker       := envOr("KAFKA_BROKER", "redpanda:9092")
	metricsTopic := envOr("METRICS_TOPIC", "bench.raw_metrics")
	ebpfTopic    := envOr("EBPF_TOPIC", "telemetry.kernel_latency")
	scoreTopic   := envOr("SCORE_TOPIC", "bench.scores")
	groupID      := envOr("CONSUMER_GROUP", "telemetry-engine")
	dbDSN        := envOr("TIMESCALEDB_URL", "")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, dbDSN)
	if err != nil {
		log.Printf("timescaledb unavailable (%v) — running without persistence", err)
	} else if st != nil {
		log.Println("timescaledb connected")
		defer st.Close()
	}

	engine := scorer.NewEngine(broker, scoreTopic, st)
	cons   := consumer.New(broker, metricsTopic, ebpfTopic, groupID, engine, st)

	log.Printf("telemetry engine starting (broker=%s)", broker)
	cons.Run(ctx)
	log.Println("telemetry engine stopped")
	os.Exit(0)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
