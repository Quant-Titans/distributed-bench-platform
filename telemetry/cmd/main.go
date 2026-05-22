package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Quant-Titans/distributed-bench-platform/telemetry/internal/consumer"
	"github.com/Quant-Titans/distributed-bench-platform/telemetry/internal/scorer"
	"github.com/Quant-Titans/distributed-bench-platform/telemetry/internal/store"
)

func main() {
	broker       := envOr("KAFKA_BROKER", "redpanda:9092")
	metricsTopic := envOr("METRICS_TOPIC", "bench.raw_metrics")
	ebpfTopic    := envOr("EBPF_TOPIC", "telemetry.kernel_latency")
	eventsTopic  := envOr("EVENTS_TOPIC", "bench.events")
	scoreTopic   := envOr("SCORE_TOPIC", "bench.scores")
	groupID      := envOr("CONSUMER_GROUP", "telemetry-engine")
	dbDSN        := envOr("TIMESCALEDB_URL", "")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st := connectStore(ctx, dbDSN)
	if st != nil {
		defer st.Close()
	}

	engine := scorer.NewEngine(broker, scoreTopic, st)
	cons   := consumer.New(broker, metricsTopic, ebpfTopic, eventsTopic, groupID, engine, st)

	log.Printf("telemetry engine starting (broker=%s)", broker)
	cons.Run(ctx)
	log.Println("telemetry engine stopped")
	os.Exit(0)
}

// connectStore retries the TimescaleDB connection with exponential backoff for
// up to 90 seconds so telemetry survives slow database startup in Kubernetes.
func connectStore(ctx context.Context, dsn string) *store.Store {
	if dsn == "" {
		return nil
	}
	backoff := 2 * time.Second
	deadline := time.Now().Add(90 * time.Second)
	for {
		st, err := store.New(ctx, dsn)
		if err == nil {
			log.Println("timescaledb connected")
			return st
		}
		if time.Now().After(deadline) {
			log.Printf("timescaledb unavailable after 90s (%v) — running without persistence", err)
			return nil
		}
		log.Printf("timescaledb not ready (%v) — retrying in %s", err, backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < 16*time.Second {
			backoff *= 2
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
