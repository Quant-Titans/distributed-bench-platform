// Package consumer reads raw metrics from Kafka and dispatches to the scorer.
package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// RawMetric is the event emitted by each bot per order, deserialized from Kafka.
type RawMetric struct {
	SessionID  string  `json:"session_id"`
	SandboxID  string  `json:"sandbox_id"`
	OrderID    string  `json:"order_id"`
	Archetype  string  `json:"archetype"`
	AppRTTNS   int64   `json:"app_rtt_ns"`
	KernelRTTNS int64  `json:"kernel_rtt_ns"` // populated by eBPF prober events
	Correct    bool    `json:"correct"`
	FillPrice  float64 `json:"fill_price"`
	FillQty    int64   `json:"fill_qty"`
	EmittedNS  int64   `json:"emitted_ns"`
	ReplaySeq  int64   `json:"replay_seq"`
}

// Handler processes a batch of metrics. Implemented by the scorer.
type Handler interface {
	Handle(ctx context.Context, metrics []RawMetric) error
}

// Consumer reads from two Kafka topics:
//   - MetricsTopic: bot app-layer metrics
//   - EBPFTopic:    kernel-level RTT events from the eBPF prober
type Consumer struct {
	metricsReader *kafka.Reader
	ebpfReader    *kafka.Reader
	handler       Handler
}

func New(broker, metricsTopic, ebpfTopic, groupID string, handler Handler) *Consumer {
	return &Consumer{
		metricsReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        []string{broker},
			Topic:          metricsTopic,
			GroupID:        groupID + "-metrics",
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: time.Second,
		}),
		ebpfReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        []string{broker},
			Topic:          ebpfTopic,
			GroupID:        groupID + "-ebpf",
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: time.Second,
		}),
		handler: handler,
	}
}

// Run starts both reader goroutines and blocks until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) {
	go c.readMetrics(ctx)
	go c.readEBPF(ctx)
	<-ctx.Done()
	_ = c.metricsReader.Close()
	_ = c.ebpfReader.Close()
}

func (c *Consumer) readMetrics(ctx context.Context) {
	batch := make([]RawMetric, 0, 64)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := c.handler.Handle(ctx, batch); err != nil {
			log.Printf("metrics handle: %v", err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-ticker.C:
			flush()
		default:
			msg, err := c.metricsReader.ReadMessage(ctx)
			if err != nil {
				return
			}
			var m RawMetric
			if err := json.Unmarshal(msg.Value, &m); err == nil {
				batch = append(batch, m)
				if len(batch) >= 64 {
					flush()
				}
			}
		}
	}
}

// readEBPF merges kernel-level RTT events into the metrics stream by order ID.
// These events arrive slightly after the app-layer metrics; we do a best-effort
// join and store kernel RTT separately in TimescaleDB.
func (c *Consumer) readEBPF(ctx context.Context) {
	for {
		msg, err := c.ebpfReader.ReadMessage(ctx)
		if err != nil {
			return
		}
		// eBPF events have a different shape — rtt_ns, session_id, sandbox_id
		var ev struct {
			SessionID string `json:"session_id"`
			SandboxID string `json:"sandbox_id"`
			RTTNS     uint64 `json:"rtt_ns"`
		}
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			continue
		}
		m := RawMetric{
			SessionID:   ev.SessionID,
			SandboxID:   ev.SandboxID,
			KernelRTTNS: int64(ev.RTTNS),
		}
		_ = c.handler.Handle(ctx, []RawMetric{m})
	}
}
