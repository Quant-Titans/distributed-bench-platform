// Leaderboard WebSocket server.
//
// Consumes bench.scores from Kafka, maintains a sorted leaderboard state,
// and broadcasts LeaderboardSnapshot to all connected browser clients
// whenever a score changes.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

// CompositeScore mirrors the telemetry scorer output published to bench.scores.
type CompositeScore struct {
	SessionID           string  `json:"session_id"`
	TeamName            string  `json:"team_name"`
	P50NS               float64 `json:"p50_ns"`
	P90NS               float64 `json:"p90_ns"`
	P99NS               float64 `json:"p99_ns"`
	P999NS              float64 `json:"p999_ns"`
	TPS                 float64 `json:"tps"`
	PeakTPS             float64 `json:"peak_tps"`
	FillAccuracy        float64 `json:"fill_accuracy"`
	PriceTimeViolations int64   `json:"price_time_violations"`
	RecoveryTimeMS      float64 `json:"recovery_time_ms"`
	DegradationRatio    float64 `json:"degradation_ratio"`
	ThroughputScore     float64 `json:"throughput_score"`
	TailLatencyScore    float64 `json:"tail_latency_score"`
	CorrectnessScore    float64 `json:"correctness_score"`
	ResilienceScore     float64 `json:"resilience_score"`
	TotalScore          float64 `json:"total_score"`
	ComputedAt          int64   `json:"computed_at_ns"`
}

type LeaderboardEntry struct {
	Rank int `json:"rank"`
	CompositeScore
}

type LeaderboardSnapshot struct {
	Entries    []LeaderboardEntry `json:"entries"`
	SnapshotNS int64              `json:"snapshot_ns"`
}

// Hub holds all active WebSocket connections and the current leaderboard state.
type Hub struct {
	mu      sync.RWMutex
	scores  map[string]*CompositeScore // session_id → latest score
	clients map[*websocket.Conn]struct{}
}

func newHub() *Hub {
	return &Hub{
		scores:  make(map[string]*CompositeScore),
		clients: make(map[*websocket.Conn]struct{}),
	}
}

func (h *Hub) upsert(score CompositeScore) {
	h.mu.Lock()
	h.scores[score.SessionID] = &score
	h.mu.Unlock()
	h.broadcast()
}

func (h *Hub) snapshot() LeaderboardSnapshot {
	h.mu.RLock()
	entries := make([]LeaderboardEntry, 0, len(h.scores))
	for _, s := range h.scores {
		entries = append(entries, LeaderboardEntry{CompositeScore: *s})
	}
	h.mu.RUnlock()

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].TotalScore > entries[j].TotalScore
	})
	for i := range entries {
		entries[i].Rank = i + 1
	}
	return LeaderboardSnapshot{Entries: entries, SnapshotNS: time.Now().UnixNano()}
}

func (h *Hub) broadcast() {
	snap := h.snapshot()
	h.mu.RLock()
	conns := make([]*websocket.Conn, 0, len(h.clients))
	for c := range h.clients {
		conns = append(conns, c)
	}
	h.mu.RUnlock()

	for _, c := range conns {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := wsjson.Write(ctx, c, snap); err != nil {
			// Client disconnected — remove it
			h.mu.Lock()
			delete(h.clients, c)
			h.mu.Unlock()
		}
		cancel()
	}
}

func (h *Hub) addClient(c *websocket.Conn) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	// Send current state immediately on connect
	snap := h.snapshot()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = wsjson.Write(ctx, c, snap)
}

func (h *Hub) removeClient(c *websocket.Conn) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

func main() {
	broker     := envOr("KAFKA_BROKER", "redpanda:9092")
	scoreTopic := envOr("SCORE_TOPIC", "bench.scores")
	groupID    := envOr("CONSUMER_GROUP", "leaderboard-ws")
	listenAddr := envOr("LISTEN_ADDR", ":8082")
	staticDir  := envOr("STATIC_DIR", "/app/dist")

	hub := newHub()

	// Kafka consumer — reads bench.scores and pushes to hub
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          scoreTopic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       1e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("leaderboard consuming %s from %s", scoreTopic, broker)
		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("kafka read: %v", err)
				time.Sleep(time.Second)
				continue
			}
			var score CompositeScore
			if err := json.Unmarshal(msg.Value, &score); err != nil {
				continue
			}
			hub.upsert(score)
		}
	}()

	mux := http.NewServeMux()

	// WebSocket endpoint — browser connects here
	mux.HandleFunc("/ws/leaderboard", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true, // allow any origin in dev/demo
		})
		if err != nil {
			log.Printf("ws accept: %v", err)
			return
		}
		defer conn.CloseNow()
		hub.addClient(conn)
		defer hub.removeClient(conn)

		// Hold connection open until client disconnects
		for {
			if _, _, err := conn.Read(r.Context()); err != nil {
				return
			}
		}
	})

	// Health check
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// Serve React static build
	mux.Handle("/", http.FileServer(http.Dir(staticDir)))

	srv := &http.Server{
		Addr:         listenAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 0, // WebSocket connections must not timeout on write
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("leaderboard server listening on %s", listenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down leaderboard server")
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = reader.Close()
	_ = srv.Shutdown(shutCtx)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
