package scorer

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/Quant-Titans/distributed-bench-platform/telemetry/internal/consumer"
	"github.com/Quant-Titans/distributed-bench-platform/telemetry/internal/store"
	kafka "github.com/segmentio/kafka-go"
)

// CompositeScore is the final benchmark result for a session.
// Formula:
//   Total = 0.30×Throughput + 0.30×TailLatency + 0.25×Correctness + 0.15×Resilience
type CompositeScore struct {
	SessionID   string  `json:"session_id"`
	TeamName    string  `json:"team_name"`
	P50NS       float64 `json:"p50_ns"`
	P90NS       float64 `json:"p90_ns"`
	P99NS       float64 `json:"p99_ns"`
	P999NS      float64 `json:"p999_ns"`
	TPS         float64 `json:"tps"`
	PeakTPS     float64 `json:"peak_tps"`
	FillAccuracy float64 `json:"fill_accuracy"`
	PriceTimeViolations int64 `json:"price_time_violations"`
	RecoveryTimeMS     float64 `json:"recovery_time_ms"`
	DegradationRatio   float64 `json:"degradation_ratio"`
	ThroughputScore  float64 `json:"throughput_score"`
	TailLatencyScore float64 `json:"tail_latency_score"`
	CorrectnessScore float64 `json:"correctness_score"`
	ResilienceScore  float64 `json:"resilience_score"`
	TotalScore       float64 `json:"total_score"`
	ComputedAt       int64   `json:"computed_at_ns"`
}

// Engine processes raw metrics and emits composite scores to Kafka.
type Engine struct {
	mu         sync.Mutex
	sessions   map[string]*sessionState
	writer     *kafka.Writer
	scoreTopic string
	store      *store.Store
}

type sessionState struct {
	hdr       *LatencyHistogram
	validator *Validator
	startedAt time.Time
	orderCount int64
	peakTPS    float64
	// Chaos resilience tracking
	chaosStartNS    int64
	chaosEndNS      int64
	baselineP99NS   float64
	chaosP99NS      float64
}

func NewEngine(broker, scoreTopic string, st *store.Store) *Engine {
	return &Engine{
		sessions: make(map[string]*sessionState),
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:      []string{broker},
			Topic:        scoreTopic,
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 100 * time.Millisecond,
		}),
		scoreTopic: scoreTopic,
		store:      st,
	}
}

// Handle implements consumer.Handler — called by the Kafka consumer.
func (e *Engine) Handle(ctx context.Context, metrics []consumer.RawMetric) error {
	e.mu.Lock()
	for _, m := range metrics {
		s := e.getOrCreateSession(m.SessionID)
		if m.AppRTTNS > 0 {
			s.hdr.RecordApp(m.AppRTTNS)
		}
		if m.KernelRTTNS > 0 {
			s.hdr.RecordKernel(m.KernelRTTNS)
		}
		if m.OrderID != "" {
			isBuy := true
			s.validator.RecordOrder(m.OrderID, m.FillPrice, isBuy)
			if m.FillPrice > 0 || m.FillQty > 0 {
				s.validator.RecordFill(m.OrderID, m.FillPrice, m.FillQty)
			}
		}
		s.orderCount++
		elapsed := time.Since(s.startedAt).Seconds()
		if elapsed > 0 {
			currentTPS := float64(s.orderCount) / elapsed
			if currentTPS > s.peakTPS {
				s.peakTPS = currentTPS
			}
		}
		// Persist raw metric to TimescaleDB (async, non-blocking)
		e.store.WriteRawMetric(store.RawMetricRow{
			SessionID:   m.SessionID,
			SandboxID:   m.SandboxID,
			OrderID:     m.OrderID,
			Archetype:   m.Archetype,
			AppRTTNS:    m.AppRTTNS,
			KernelRTTNS: m.KernelRTTNS,
			FillCorrect: m.Correct,
			FillPrice:   m.FillPrice,
			FillQty:     m.FillQty,
			ReplaySeq:   m.ReplaySeq,
		})
	}
	e.mu.Unlock()

	// Emit updated score for all touched sessions
	seen := make(map[string]struct{})
	for _, m := range metrics {
		if _, ok := seen[m.SessionID]; ok {
			continue
		}
		seen[m.SessionID] = struct{}{}
		score := e.Compute(m.SessionID)
		if err := e.publish(ctx, score); err != nil {
			return fmt.Errorf("publish score: %w", err)
		}
	}
	return nil
}

// Compute calculates the composite score for a session.
func (e *Engine) Compute(sessionID string) CompositeScore {
	e.mu.Lock()
	s, ok := e.sessions[sessionID]
	e.mu.Unlock()

	if !ok {
		return CompositeScore{SessionID: sessionID}
	}

	p50, p90, p99, p999 := s.hdr.KernelPercentiles()
	total, correct, violations := s.validator.Stats()

	elapsed := time.Since(s.startedAt).Seconds()
	tps := 0.0
	if elapsed > 0 {
		tps = float64(s.orderCount) / elapsed
	}

	fillAccuracy := 0.0
	if total > 0 {
		fillAccuracy = float64(correct) / float64(total)
	}

	// Normalize scores 0–100
	throughputScore  := normalizeLinear(tps, 0, 50000)      // 50k TPS = 100
	tailLatencyScore := normalizeTailLatency(p99)            // lower = better
	correctnessScore := fillAccuracy * 100
	if violations > 0 {
		correctnessScore *= math.Pow(0.9, float64(violations)) // penalise per violation
	}

	degradationRatio := 1.0
	recoveryTimeMS := 0.0
	if s.chaosEndNS > 0 && s.baselineP99NS > 0 {
		degradationRatio = s.chaosP99NS / s.baselineP99NS
		recoveryTimeMS = float64(s.chaosEndNS-s.chaosStartNS) / 1e6
	}
	resilienceScore := normalizeResilience(recoveryTimeMS, degradationRatio)

	total_score := 0.30*throughputScore + 0.30*tailLatencyScore +
		0.25*correctnessScore + 0.15*resilienceScore

	return CompositeScore{
		SessionID:           sessionID,
		P50NS:               p50,
		P90NS:               p90,
		P99NS:               p99,
		P999NS:              p999,
		TPS:                 tps,
		PeakTPS:             s.peakTPS,
		FillAccuracy:        fillAccuracy,
		PriceTimeViolations: violations,
		RecoveryTimeMS:      recoveryTimeMS,
		DegradationRatio:    degradationRatio,
		ThroughputScore:     throughputScore,
		TailLatencyScore:    tailLatencyScore,
		CorrectnessScore:    correctnessScore,
		ResilienceScore:     resilienceScore,
		TotalScore:          total_score,
		ComputedAt:          time.Now().UnixNano(),
	}
}

func (e *Engine) publish(ctx context.Context, score CompositeScore) error {
	b, err := json.Marshal(score)
	if err != nil {
		return err
	}
	// Persist composite score to TimescaleDB (async, non-blocking)
	e.store.WriteCompositeScore(store.CompositeScoreRow{
		SessionID:           score.SessionID,
		TeamName:            score.TeamName,
		P50NS:               score.P50NS,
		P90NS:               score.P90NS,
		P99NS:               score.P99NS,
		P999NS:              score.P999NS,
		TPS:                 score.TPS,
		PeakTPS:             score.PeakTPS,
		FillAccuracy:        score.FillAccuracy,
		PriceTimeViolations: score.PriceTimeViolations,
		RecoveryTimeMS:      score.RecoveryTimeMS,
		DegradationRatio:    score.DegradationRatio,
		ThroughputScore:     score.ThroughputScore,
		TailLatencyScore:    score.TailLatencyScore,
		CorrectnessScore:    score.CorrectnessScore,
		ResilienceScore:     score.ResilienceScore,
		TotalScore:          score.TotalScore,
	})
	return e.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(score.SessionID),
		Value: b,
	})
}

func (e *Engine) getOrCreateSession(id string) *sessionState {
	if s, ok := e.sessions[id]; ok {
		return s
	}
	s := &sessionState{
		hdr:       NewLatencyHistogram(),
		validator: NewValidator(),
		startedAt: time.Now(),
	}
	e.sessions[id] = s
	return s
}

// ── Normalisation helpers ─────────────────────────────────────────────────────

func normalizeLinear(v, min, max float64) float64 {
	if max == min {
		return 0
	}
	return math.Min(100, math.Max(0, (v-min)/(max-min)*100))
}

// normalizeTailLatency: 100 at p99≤1µs, 0 at p99≥100ms (log scale).
func normalizeTailLatency(p99ns float64) float64 {
	if p99ns <= 0 {
		return 0
	}
	const best = 1_000.0     // 1µs
	const worst = 100_000_000.0 // 100ms
	logP99 := math.Log10(p99ns)
	logBest := math.Log10(best)
	logWorst := math.Log10(worst)
	score := 100 * (1 - (logP99-logBest)/(logWorst-logBest))
	return math.Min(100, math.Max(0, score))
}

// normalizeResilience: 100 if recovery<100ms and degradation<1.5×, sliding down from there.
func normalizeResilience(recoveryMS, degradationRatio float64) float64 {
	if recoveryMS == 0 {
		return 85 // full chaos run not completed yet
	}
	recoveryScore := normalizeLinear(recoveryMS, 0, 5000) // 5s = worst
	recoveryScore = 100 - recoveryScore
	degradScore := math.Max(0, 100-math.Max(0, degradationRatio-1)*50)
	return (recoveryScore + degradScore) / 2
}
