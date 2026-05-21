// Package consumer reads raw metrics from Kafka and dispatches to the scorer.
package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Quant-Titans/distributed-bench-platform/telemetry/internal/store"
	kafka "github.com/segmentio/kafka-go"
)

// RawMetric is the event emitted by each bot per order, deserialized from Kafka.
type RawMetric struct {
	SessionID   string  `json:"session_id"`
	SandboxID   string  `json:"sandbox_id"`
	OrderID     string  `json:"order_id"`
	Archetype   string  `json:"archetype"`
	AppRTTNS    int64   `json:"app_rtt_ns"`
	KernelRTTNS int64   `json:"kernel_rtt_ns"` // populated by eBPF prober events
	Correct     bool    `json:"correct"`
	FillPrice   float64 `json:"fill_price"`
	FillQty     int64   `json:"fill_qty"`
	EmittedNS   int64   `json:"emitted_ns"`
	ReplaySeq   int64   `json:"replay_seq"`
}

// ChaosEvent is published to bench.events by the sandbox manager when a fault
// schedule starts or ends.
type ChaosEvent struct {
	SessionID   string `json:"session_id"`
	SandboxID   string `json:"sandbox_id"`
	EventType   string `json:"event_type"`   // "chaos_start" | "chaos_end"
	FaultType   string `json:"fault_type"`
	TimestampNS int64  `json:"timestamp_ns"`
}

// Handler processes metrics and chaos lifecycle events. Implemented by the scorer.
type Handler interface {
	Handle(ctx context.Context, metrics []RawMetric) error
	HandleChaosEvent(ctx context.Context, ev ChaosEvent) error
}

// Consumer reads from three Kafka topics:
//   - MetricsTopic: bot app-layer metrics
//   - EBPFTopic:    kernel-level RTT events from the eBPF prober
//   - EventsTopic:  chaos lifecycle events from the sandbox manager
type Consumer struct {
	metricsReader *kafka.Reader
	ebpfReader    *kafka.Reader
	eventsReader  *kafka.Reader
	handler       Handler
	store         *store.Store
}

func New(broker, metricsTopic, ebpfTopic, eventsTopic, groupID string, handler Handler, st *store.Store) *Consumer {
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
		eventsReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        []string{broker},
			Topic:          eventsTopic,
			GroupID:        groupID + "-events",
			MinBytes:       1,
			MaxBytes:       1e6,
			CommitInterval: time.Second,
		}),
		handler: handler,
		store:   st,
	}
}

// Run starts all reader goroutines and blocks until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) {
	go c.readMetrics(ctx)
	go c.readEBPF(ctx)
	go c.readEvents(ctx)
	<-ctx.Done()
	_ = c.metricsReader.Close()
	_ = c.ebpfReader.Close()
	_ = c.eventsReader.Close()
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

// readEBPF merges kernel-level RTT events into the metrics stream and persists
// them to the kernel_latency hypertable.
func (c *Consumer) readEBPF(ctx context.Context) {
	for {
		msg, err := c.ebpfReader.ReadMessage(ctx)
		if err != nil {
			return
		}
		var ev struct {
			SessionID string `json:"session_id"`
			SandboxID string `json:"sandbox_id"`
			RTTNS     uint64 `json:"rtt_ns"`
			IngressNS uint64 `json:"ingress_ns"`
			EgressNS  uint64 `json:"egress_ns"`
			SrcIP     string `json:"src_ip"`
			DstIP     string `json:"dst_ip"`
			SrcPort   int    `json:"src_port"`
			DstPort   int    `json:"dst_port"`
		}
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			continue
		}
		// Persist kernel RTT to TimescaleDB (async, non-blocking)
		c.store.WriteKernelLatency(store.KernelLatencyRow{
			SessionID: ev.SessionID,
			SandboxID: ev.SandboxID,
			SrcIP:     ev.SrcIP,
			DstIP:     ev.DstIP,
			SrcPort:   ev.SrcPort,
			DstPort:   ev.DstPort,
			RTTNS:     int64(ev.RTTNS),
			IngressNS: int64(ev.IngressNS),
			EgressNS:  int64(ev.EgressNS),
		})
		_ = c.handler.Handle(ctx, []RawMetric{{
			SessionID:   ev.SessionID,
			SandboxID:   ev.SandboxID,
			KernelRTTNS: int64(ev.RTTNS),
		}})
	}
}

// readEvents consumes bench.events — chaos lifecycle messages from the sandbox
// manager — and forwards them to the scorer to update chaos tracking state.
func (c *Consumer) readEvents(ctx context.Context) {
	for {
		msg, err := c.eventsReader.ReadMessage(ctx)
		if err != nil {
			return
		}
		var ev ChaosEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			continue
		}
		if ev.EventType != "chaos_start" && ev.EventType != "chaos_end" {
			continue
		}
		if err := c.handler.HandleChaosEvent(ctx, ev); err != nil {
			log.Printf("chaos event handle: %v", err)
		}
	}
}
