// Leaderboard WebSocket server.
//
// Consumes bench.scores and bench.events from Kafka, maintains a sorted
// leaderboard state with chaos tracking, and broadcasts LeaderboardSnapshot
// to all connected browser clients on every score change.
//
// Optional: TIMESCALE_DSN enables the /api/timeseries HTTP endpoint.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafka "github.com/segmentio/kafka-go"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

// CompositeScore mirrors the telemetry scorer output published to bench.scores.
type CompositeScore struct {
	SessionID           string           `json:"session_id"`
	TeamName            string           `json:"team_name"`
	P50NS               float64          `json:"p50_ns"`
	P90NS               float64          `json:"p90_ns"`
	P99NS               float64          `json:"p99_ns"`
	P999NS              float64          `json:"p999_ns"`
	TPS                 float64          `json:"tps"`
	PeakTPS             float64          `json:"peak_tps"`
	FillAccuracy        float64          `json:"fill_accuracy"`
	PriceTimeViolations int64            `json:"price_time_violations"`
	RecoveryTimeMS      float64          `json:"recovery_time_ms"`
	DegradationRatio    float64          `json:"degradation_ratio"`
	ThroughputScore     float64          `json:"throughput_score"`
	TailLatencyScore    float64          `json:"tail_latency_score"`
	CorrectnessScore    float64          `json:"correctness_score"`
	ResilienceScore     float64          `json:"resilience_score"`
	TotalScore          float64          `json:"total_score"`
	ActiveBots          int32            `json:"active_bots"`
	ScoreHistory        []float64        `json:"score_history"`
	ArchetypeCounts     map[string]int32 `json:"archetype_counts"`
	ComputedAt          int64            `json:"computed_at_ns"`
}

type LeaderboardEntry struct {
	Rank        int  `json:"rank"`
	ChaosActive bool `json:"chaos_active"`
	CompositeScore
}

type LeaderboardSnapshot struct {
	Entries    []LeaderboardEntry `json:"entries"`
	SnapshotNS int64              `json:"snapshot_ns"`
}

// Hub holds all active WebSocket connections and the current leaderboard state.
type Hub struct {
	mu          sync.RWMutex
	scores      map[string]*CompositeScore // session_id → latest score
	chaosActive map[string]bool            // session_id → chaos currently active
	clients     map[*websocket.Conn]struct{}
}

func newHub() *Hub {
	return &Hub{
		scores:      make(map[string]*CompositeScore),
		chaosActive: make(map[string]bool),
		clients:     make(map[*websocket.Conn]struct{}),
	}
}

func (h *Hub) upsert(score CompositeScore) {
	h.mu.Lock()
	h.scores[score.SessionID] = &score
	h.mu.Unlock()
	h.broadcast()
}

func (h *Hub) setChaos(sessionID string, active bool) {
	h.mu.Lock()
	h.chaosActive[sessionID] = active
	h.mu.Unlock()
	h.broadcast()
}

func (h *Hub) snapshot() LeaderboardSnapshot {
	h.mu.RLock()
	entries := make([]LeaderboardEntry, 0, len(h.scores))
	for _, s := range h.scores {
		entries = append(entries, LeaderboardEntry{
			CompositeScore: *s,
			ChaosActive:    h.chaosActive[s.SessionID],
		})
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
	broker       := envOr("KAFKA_BROKER", "redpanda:9092")
	scoreTopic   := envOr("SCORE_TOPIC", "bench.scores")
	eventsTopic  := envOr("EVENTS_TOPIC", "bench.events")
	listenAddr   := envOr("LISTEN_ADDR", ":8082")
	staticDir    := envOr("STATIC_DIR", "/app/dist")
	timescaleDSN := envOr("TIMESCALE_DSN", "")

	hub := newHub()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Scores consumer — replays from earliest offset so hub rebuilds full state on restart.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       scoreTopic,
		MinBytes:    1,
		MaxBytes:    1e6,
		StartOffset: kafka.FirstOffset,
	})

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

	// Events consumer — tracks chaos_start / chaos_end per session.
	eventsReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       eventsTopic,
		MinBytes:    1,
		MaxBytes:    1e6,
		StartOffset: kafka.LastOffset,
	})

	go func() {
		log.Printf("leaderboard consuming events from %s/%s", broker, eventsTopic)
		for {
			msg, err := eventsReader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("events kafka read: %v", err)
				time.Sleep(time.Second)
				continue
			}
			var ev struct {
				SessionID string `json:"session_id"`
				EventType string `json:"event_type"`
			}
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				continue
			}
			switch ev.EventType {
			case "chaos_start":
				hub.setChaos(ev.SessionID, true)
			case "chaos_end":
				hub.setChaos(ev.SessionID, false)
			}
		}
	}()

	// Optional TimescaleDB connection pool for /api/timeseries endpoint.
	var pool *pgxpool.Pool
	if timescaleDSN != "" {
		p, err := pgxpool.New(ctx, timescaleDSN)
		if err != nil {
			log.Printf("timescaledb connect: %v (timeseries endpoint disabled)", err)
		} else {
			pool = p
			log.Printf("timescaledb connected for timeseries queries")
		}
	}

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

	// /api/timeseries?session_id=X&metric=p99&limit=100
	// Queries composite_scores hypertable for a historical view of any metric.
	mux.HandleFunc("/api/timeseries", func(w http.ResponseWriter, r *http.Request) {
		if pool == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"timescaledb not configured"}`))
			return
		}
		sessionID := r.URL.Query().Get("session_id")
		metric    := r.URL.Query().Get("metric")
		limitStr  := r.URL.Query().Get("limit")

		limit := 100
		if limitStr != "" {
			if n, err := strconv.Atoi(limitStr); err == nil && n > 0 && n <= 1000 {
				limit = n
			}
		}
		if sessionID == "" || metric == "" {
			http.Error(w, `{"error":"session_id and metric required"}`, http.StatusBadRequest)
			return
		}

		// Allowlist to prevent SQL injection via the column name.
		allowed := map[string]string{
			"p50":   "p50_ns",
			"p90":   "p90_ns",
			"p99":   "p99_ns",
			"p999":  "p999_ns",
			"tps":   "tps",
			"total": "total_score",
		}
		col, ok := allowed[metric]
		if !ok {
			http.Error(w, `{"error":"unknown metric; valid: p50 p90 p99 p999 tps total"}`, http.StatusBadRequest)
			return
		}

		query := fmt.Sprintf(
			`SELECT extract(epoch from time)*1000 AS ts_ms, %s AS value
			 FROM composite_scores
			 WHERE session_id = $1
			 ORDER BY time DESC
			 LIMIT $2`, col)

		rows, err := pool.Query(r.Context(), query, sessionID, limit)
		if err != nil {
			log.Printf("timeseries query: %v", err)
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		type point struct {
			TsMs  float64 `json:"ts_ms"`
			Value float64 `json:"value"`
		}
		var pts []point
		for rows.Next() {
			var p point
			if err := rows.Scan(&p.TsMs, &p.Value); err != nil {
				continue
			}
			pts = append(pts, p)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"session_id": sessionID,
			"metric":     metric,
			"points":     pts,
		})
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
	_ = eventsReader.Close()
	if pool != nil {
		pool.Close()
	}
	_ = srv.Shutdown(shutCtx)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
