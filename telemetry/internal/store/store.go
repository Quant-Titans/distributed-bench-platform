// Package store handles async writes to TimescaleDB hypertables.
// All public Write* methods are fire-and-forget: they enqueue work on a
// buffered channel and return immediately so the scoring hot-path is never
// blocked by database latency.
package store

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RawMetricRow mirrors the raw_metrics hypertable columns.
type RawMetricRow struct {
	SessionID   string
	SandboxID   string
	OrderID     string
	Archetype   string
	AppRTTNS    int64
	KernelRTTNS int64
	FillCorrect bool
	FillPrice   float64
	FillQty     int64
	ReplaySeq   int64
}

// CompositeScoreRow mirrors the composite_scores hypertable columns.
type CompositeScoreRow struct {
	SessionID           string
	TeamName            string
	P50NS               float64
	P90NS               float64
	P99NS               float64
	P999NS              float64
	TPS                 float64
	PeakTPS             float64
	FillAccuracy        float64
	PriceTimeViolations int64
	RecoveryTimeMS      float64
	DegradationRatio    float64
	ThroughputScore     float64
	TailLatencyScore    float64
	CorrectnessScore    float64
	ResilienceScore     float64
	TotalScore          float64
}

// KernelLatencyRow mirrors the kernel_latency hypertable columns.
type KernelLatencyRow struct {
	SessionID string
	SandboxID string
	SrcIP     string
	DstIP     string
	SrcPort   int
	DstPort   int
	RTTNS     int64
	IngressNS int64
	EgressNS  int64
}

type writeJob struct {
	kind string // "raw" | "score" | "kernel"
	raw  *RawMetricRow
	score *CompositeScoreRow
	kernel *KernelLatencyRow
}

// Store wraps a pgxpool and provides non-blocking writes to TimescaleDB.
type Store struct {
	pool *pgxpool.Pool
	ch   chan writeJob
}

// New connects to TimescaleDB and starts the background writer goroutine.
// Returns (nil, nil) if dsn is empty — caller treats nil Store as "no-op".
func New(ctx context.Context, dsn string) (*Store, error) {
	if dsn == "" {
		return nil, nil
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	s := &Store{
		pool: pool,
		ch:   make(chan writeJob, 8192),
	}
	go s.worker()
	return s, nil
}

// WriteRawMetric enqueues a raw_metrics row for async insertion.
func (s *Store) WriteRawMetric(r RawMetricRow) {
	if s == nil {
		return
	}
	select {
	case s.ch <- writeJob{kind: "raw", raw: &r}:
	default:
		log.Println("store: raw_metrics channel full, dropping row")
	}
}

// WriteCompositeScore enqueues a composite_scores row for async insertion.
func (s *Store) WriteCompositeScore(r CompositeScoreRow) {
	if s == nil {
		return
	}
	select {
	case s.ch <- writeJob{kind: "score", score: &r}:
	default:
		log.Println("store: composite_scores channel full, dropping row")
	}
}

// WriteKernelLatency enqueues a kernel_latency row for async insertion.
func (s *Store) WriteKernelLatency(r KernelLatencyRow) {
	if s == nil {
		return
	}
	select {
	case s.ch <- writeJob{kind: "kernel", kernel: &r}:
	default:
		log.Println("store: kernel_latency channel full, dropping row")
	}
}

// Close drains the write queue and closes the pool.
func (s *Store) Close() {
	if s == nil {
		return
	}
	close(s.ch)
	s.pool.Close()
}

// worker drains the channel and executes DB inserts sequentially.
// A single worker is sufficient because TimescaleDB batch inserts via a
// connection pool are fast; the bottleneck is network, not concurrency.
func (s *Store) worker() {
	ctx := context.Background()
	for job := range s.ch {
		switch job.kind {
		case "raw":
			s.insertRawMetric(ctx, job.raw)
		case "score":
			s.insertCompositeScore(ctx, job.score)
		case "kernel":
			s.insertKernelLatency(ctx, job.kernel)
		}
	}
}

func (s *Store) insertRawMetric(ctx context.Context, r *RawMetricRow) {
	const q = `
INSERT INTO raw_metrics
  (time, session_id, sandbox_id, order_id, archetype,
   app_rtt_ns, kernel_rtt_ns, fill_correct, fill_price, fill_qty, replay_seq)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT DO NOTHING`
	if _, err := s.pool.Exec(ctx, q,
		time.Now(), r.SessionID, r.SandboxID, r.OrderID, r.Archetype,
		r.AppRTTNS, r.KernelRTTNS, r.FillCorrect, r.FillPrice, r.FillQty, r.ReplaySeq,
	); err != nil {
		log.Printf("store: insert raw_metric: %v", err)
	}
}

func (s *Store) insertCompositeScore(ctx context.Context, r *CompositeScoreRow) {
	const q = `
INSERT INTO composite_scores
  (time, session_id, team_name, p50_ns, p90_ns, p99_ns, p999_ns,
   tps, peak_tps, fill_accuracy, price_time_violations,
   recovery_time_ms, degradation_ratio,
   throughput_score, tail_latency_score, correctness_score,
   resilience_score, total_score)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`
	if _, err := s.pool.Exec(ctx, q,
		time.Now(), r.SessionID, r.TeamName,
		r.P50NS, r.P90NS, r.P99NS, r.P999NS,
		r.TPS, r.PeakTPS, r.FillAccuracy, r.PriceTimeViolations,
		r.RecoveryTimeMS, r.DegradationRatio,
		r.ThroughputScore, r.TailLatencyScore, r.CorrectnessScore,
		r.ResilienceScore, r.TotalScore,
	); err != nil {
		log.Printf("store: insert composite_score: %v", err)
	}
}

func (s *Store) insertKernelLatency(ctx context.Context, r *KernelLatencyRow) {
	const q = `
INSERT INTO kernel_latency
  (time, session_id, sandbox_id, src_ip, dst_ip,
   src_port, dst_port, rtt_ns, ingress_ns, egress_ns)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`
	if _, err := s.pool.Exec(ctx, q,
		time.Now(), r.SessionID, r.SandboxID,
		r.SrcIP, r.DstIP, r.SrcPort, r.DstPort,
		r.RTTNS, r.IngressNS, r.EgressNS,
	); err != nil {
		log.Printf("store: insert kernel_latency: %v", err)
	}
}
