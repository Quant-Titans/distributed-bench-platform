package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// ArchetypeMix defines how many bots of each archetype to spawn.
type ArchetypeMix struct {
	MarketMakers         int
	MomentumTraders      int
	NoiseTraders         int
	InstitutionalSlicers int
	LatencyArbs          int
}

// FleetConfig is the top-level configuration for a benchmark run.
type FleetConfig struct {
	FleetID     string
	SessionID   string
	Symbol      string
	EndpointURL string
	Mix         ArchetypeMix
	TargetTPS   int
	DurationSec int64
	KafkaBroker string
	MetricsTopic string
	ReplayTopic  string
}

// Fleet manages the lifecycle of a bot swarm.
type Fleet struct {
	cfg         FleetConfig
	activeCount atomic.Int32
	ordersSent  atomic.Int64
	writer      *kafka.Writer
	replaySeq   atomic.Int64
}

func NewFleet(cfg FleetConfig) *Fleet {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{cfg.KafkaBroker},
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 5 * time.Millisecond,
	})
	return &Fleet{cfg: cfg, writer: w}
}

// Run spawns all bots, collects metrics, publishes to Kafka, and returns when
// the fleet finishes (duration elapsed or ctx cancelled).
func (f *Fleet) Run(ctx context.Context) error {
	defer f.writer.Close()

	runCtx, cancel := context.WithTimeout(ctx, time.Duration(f.cfg.DurationSec)*time.Second)
	defer cancel()

	metricsCh := make(chan Metrics, 4096)

	var wg sync.WaitGroup
	f.spawnBots(runCtx, &wg, metricsCh)

	// Drain metrics channel and publish to Kafka.
	go f.publishMetrics(runCtx, metricsCh)

	wg.Wait()
	close(metricsCh)
	return nil
}

func (f *Fleet) spawnBots(ctx context.Context, wg *sync.WaitGroup, out chan<- Metrics) {
	perBotTPS := f.cfg.TargetTPS / max(f.totalBots(), 1)

	spawn := func(bot Bot) {
		wg.Add(1)
		f.activeCount.Add(1)
		go func() {
			defer wg.Done()
			defer f.activeCount.Add(-1)
			_ = bot.Run(ctx, f.cfg.EndpointURL, out)
		}()
	}

	botCfg := Config{Symbol: f.cfg.Symbol, TargetTPS: perBotTPS, SessionID: f.cfg.SessionID}
	mix := f.cfg.Mix

	for i := 0; i < mix.MarketMakers; i++ {
		spawn(NewMarketMakerBot(botCfg))
	}
	for i := 0; i < mix.MomentumTraders; i++ {
		spawn(NewMomentumBot(botCfg))
	}
	for i := 0; i < mix.NoiseTraders; i++ {
		spawn(NewNoiseBot(botCfg))
	}
	for i := 0; i < mix.InstitutionalSlicers; i++ {
		spawn(NewInstitutionalSlicerBot(botCfg))
	}
	for i := 0; i < mix.LatencyArbs; i++ {
		spawn(NewLatencyArbBot(botCfg))
	}
}

func (f *Fleet) publishMetrics(ctx context.Context, ch <-chan Metrics) {
	for m := range ch {
		f.ordersSent.Add(1)
		seq := f.replaySeq.Add(1)

		msg, err := json.Marshal(map[string]any{
			"session_id":  f.cfg.SessionID,
			"fleet_id":    f.cfg.FleetID,
			"order_id":    m.OrderID,
			"archetype":   m.Archetype.String(),
			"app_rtt_ns":  m.AppRTTNS,
			"correct":     m.Correct,
			"fill_price":  m.FillPrice,
			"fill_qty":    m.FillQty,
			"emitted_ns":  m.EmittedNS,
			"replay_seq":  seq,  // monotonic sequence for deterministic replay
		})
		if err != nil {
			continue
		}

		// Write to both metrics topic and replay topic simultaneously.
		_ = f.writer.WriteMessages(ctx,
			kafka.Message{Topic: f.cfg.MetricsTopic, Key: []byte(f.cfg.SessionID), Value: msg},
			kafka.Message{Topic: f.cfg.ReplayTopic, Key: []byte(fmt.Sprintf("%d", seq)), Value: msg},
		)
	}
}

func (f *Fleet) ActiveBots() int32    { return f.activeCount.Load() }
func (f *Fleet) OrdersSent() int64    { return f.ordersSent.Load() }

func (f *Fleet) totalBots() int {
	m := f.cfg.Mix
	return m.MarketMakers + m.MomentumTraders + m.NoiseTraders + m.InstitutionalSlicers + m.LatencyArbs
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
