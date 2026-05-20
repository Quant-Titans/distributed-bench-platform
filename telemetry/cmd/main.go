package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Quant-Titans/distributed-bench-platform/telemetry/internal/consumer"
	"github.com/Quant-Titans/distributed-bench-platform/telemetry/internal/scorer"
)

func main() {
	broker       := envOr("KAFKA_BROKER", "redpanda:9092")
	metricsTopic := envOr("METRICS_TOPIC", "bench.raw_metrics")
	ebpfTopic    := envOr("EBPF_TOPIC", "telemetry.kernel_latency")
	eventsTopic  := envOr("EVENTS_TOPIC", "bench.events")
	scoreTopic   := envOr("SCORE_TOPIC", "bench.scores")
	groupID      := envOr("CONSUMER_GROUP", "telemetry-engine")

	engine := scorer.NewEngine(broker, scoreTopic)
	cons   := consumer.New(broker, metricsTopic, ebpfTopic, eventsTopic, groupID, engine)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
