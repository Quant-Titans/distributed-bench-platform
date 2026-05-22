package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
	FIXNoise             int // FIX 4.4 noise bots
}

// FleetConfig is the top-level configuration for a benchmark run.
type FleetConfig struct {
	FleetID      string
	SessionID    string
	TeamName     string
	Symbol       string
	EndpointURL  string
	FIXEndpoint  string // TCP addr for FIX acceptor (host:port)
	Mix          ArchetypeMix
	TargetTPS    int
	DurationSec  int64
	KafkaBroker  string
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
	httpClient  *http.Client
}

func NewFleet(cfg FleetConfig) *Fleet {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{cfg.KafkaBroker},
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 5 * time.Millisecond,
		BatchSize:    256, // batch up to 256 messages before flushing
		Async:        true,
	})

	// Single transport shared by every bot in this fleet.
	// MaxConnsPerHost caps concurrent TCP connections to the contestant endpoint,
	// providing natural backpressure without a separate semaphore.
	transport := &http.Transport{
		MaxIdleConns:        4096,
		MaxIdleConnsPerHost: 1024,
		MaxConnsPerHost:     1024,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: transport,
	}

	return &Fleet{cfg: cfg, writer: w, httpClient: client}
}

// Run spawns all bots, publishes metrics to Kafka, and returns when the fleet
// finishes (duration elapsed or ctx cancelled).
func (f *Fleet) Run(ctx context.Context) error {
	defer f.writer.Close()

	log.Printf("fleet: starting fleet=%s session=%s endpoint=%s bots=%d tps=%d duration=%ds",
		f.cfg.FleetID, f.cfg.SessionID, f.cfg.EndpointURL,
		f.totalBots(), f.cfg.TargetTPS, f.cfg.DurationSec)

	runCtx, cancel := context.WithTimeout(ctx, time.Duration(f.cfg.DurationSec)*time.Second)
	defer cancel()

	// Large buffer avoids bots blocking on back-pressure during Kafka flush.
	metricsCh := make(chan Metrics, 65536)

	var wg sync.WaitGroup
	f.spawnBots(runCtx, &wg, metricsCh)

	pubDone := make(chan struct{})
	go func() {
		f.publishMetrics(context.Background(), metricsCh)
		close(pubDone)
	}()

	wg.Wait()
	log.Printf("fleet: all bots done fleet=%s orders_sent=%d", f.cfg.FleetID, f.ordersSent.Load())
	close(metricsCh)
	<-pubDone
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

	botCfg := Config{
		Symbol:      f.cfg.Symbol,
		TargetTPS:   perBotTPS,
		SessionID:   f.cfg.SessionID,
		HTTPClient:  f.httpClient,
		FIXEndpoint: f.cfg.FIXEndpoint,
	}
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
	for i := 0; i < mix.FIXNoise; i++ {
		spawn(NewFIXNoiseBot(botCfg))
	}
}

// publishMetrics batches metrics from the channel and writes to Kafka.
// Up to 256 messages are batched per WriteMessages call, flushing at most
// every 5 ms — keeping Kafka write amplification low at high TPS.
func (f *Fleet) publishMetrics(ctx context.Context, ch <-chan Metrics) {
	batch := make([]kafka.Message, 0, 256)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	var totalFlushed int

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := f.writer.WriteMessages(ctx, batch...); err != nil {
			log.Printf("fleet: kafka write error fleet=%s: %v", f.cfg.FleetID, err)
		} else {
			totalFlushed += len(batch)
			if totalFlushed%100 == 0 || totalFlushed <= 10 {
				log.Printf("fleet: flushed %d messages total fleet=%s", totalFlushed, f.cfg.FleetID)
			}
		}
		batch = batch[:0]
	}

	for {
		select {
		case m, ok := <-ch:
			if !ok {
				flush()
				return
			}
			f.ordersSent.Add(1)
			seq := f.replaySeq.Add(1)

			payload, err := json.Marshal(map[string]any{
				"session_id":  f.cfg.SessionID,
				"team_name":   f.cfg.TeamName,
				"fleet_id":    f.cfg.FleetID,
				"order_id":    m.OrderID,
				"archetype":   m.Archetype.String(),
				"app_rtt_ns":  m.AppRTTNS,
				"correct":     m.Correct,
				"fill_price":  m.FillPrice,
				"fill_qty":    m.FillQty,
				"emitted_ns":  m.EmittedNS,
				"replay_seq":  seq,
				"active_bots": f.activeCount.Load(),
			})
			if err != nil {
				continue
			}

			batch = append(batch,
				kafka.Message{Topic: f.cfg.MetricsTopic, Key: []byte(f.cfg.SessionID), Value: payload},
				kafka.Message{Topic: f.cfg.ReplayTopic, Key: []byte(fmt.Sprintf("%d", seq)), Value: payload},
			)
			if len(batch) >= 256 {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}

func (f *Fleet) ActiveBots() int32  { return f.activeCount.Load() }
func (f *Fleet) OrdersSent() int64  { return f.ordersSent.Load() }
func (f *Fleet) SessionID() string  { return f.cfg.SessionID }

func (f *Fleet) totalBots() int {
	m := f.cfg.Mix
	return m.MarketMakers + m.MomentumTraders + m.NoiseTraders +
		m.InstitutionalSlicers + m.LatencyArbs + m.FIXNoise
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
