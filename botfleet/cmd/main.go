package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/Quant-Titans/distributed-bench-platform/botfleet/internal/grpcserver"
	"github.com/Quant-Titans/distributed-bench-platform/botfleet/internal/worker"
	"google.golang.org/grpc"
)

func main() {
	kafkaBroker  := envOr("KAFKA_BROKER", "redpanda:9092")
	metricsTopic := envOr("METRICS_TOPIC", "bench.raw_metrics")
	replayTopic  := envOr("REPLAY_TOPIC", "bench.replay_log")
	fixEndpoint  := envOr("FIX_ENDPOINT", "") // optional: host:port of FIX acceptor
	listenAddr   := envOr("LISTEN_ADDR", ":9090")

	fleetCfg := worker.FleetConfig{
		KafkaBroker:  kafkaBroker,
		MetricsTopic: metricsTopic,
		ReplayTopic:  replayTopic,
		FIXEndpoint:  fixEndpoint,
	}

	srv := grpcserver.New(fleetCfg)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}

	grpcSrv := grpc.NewServer()
	srv.Register(grpcSrv)

	go func() {
		log.Printf("botfleet gRPC listening on %s", listenAddr)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down bot fleet")
	grpcSrv.GracefulStop()

	_, cancel := context.WithTimeout(context.Background(), 0)
	cancel()
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
