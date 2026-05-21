package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	httpAddr     := envOr("HTTP_ADDR", ":9091")

	fleetCfg := worker.FleetConfig{
		KafkaBroker:  kafkaBroker,
		MetricsTopic: metricsTopic,
		ReplayTopic:  replayTopic,
		FIXEndpoint:  fixEndpoint,
	}

	srv := grpcserver.New(fleetCfg)

	// gRPC server
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

	// HTTP server — thin REST wrapper so the sandbox can trigger fleets without gRPC
	httpMux := http.NewServeMux()
	httpMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	httpMux.HandleFunc("POST /v1/spawn", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			SessionID   string `json:"session_id"`
			TeamName    string `json:"team_name"`
			EndpointURL string `json:"endpoint_url"`
			Symbol      string `json:"symbol"`
			BotCount    int32  `json:"bot_count"`
			TargetTPS   int32  `json:"target_tps"`
			DurationSec int64  `json:"duration_secs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		if req.Symbol == "" {
			req.Symbol = "AAPL"
		}
		if req.BotCount == 0 {
			req.BotCount = 200
		}
		if req.TargetTPS == 0 {
			req.TargetTPS = 1000
		}
		if req.DurationSec == 0 {
			req.DurationSec = 60
		}
		fleetResp, err := srv.SpawnFleet(r.Context(), grpcserver.SpawnFleetRequest{
			SessionID:   req.SessionID,
			TeamName:    req.TeamName,
			EndpointURL: req.EndpointURL,
			Symbol:      req.Symbol,
			BotCount:    req.BotCount,
			TargetTPS:   req.TargetTPS,
			DurationSec: req.DurationSec,
		})
		if err != nil {
			log.Printf("spawn fleet: %v", err)
			http.Error(w, `{"error":"spawn failed"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"fleet_id": fleetResp.FleetID})
	})

	httpSrv := &http.Server{
		Addr:         httpAddr,
		Handler:      httpMux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("botfleet HTTP listening on %s", httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http serve: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down bot fleet")
	grpcSrv.GracefulStop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
