//go:build linux

// Package ebpf attaches TC eBPF hooks to the sandbox bridge interface and
// publishes kernel-level RTT events to Kafka.
//
// The eBPF program bypasses Go runtime scheduling jitter, providing true
// nanosecond-precision round-trip times that application-layer timing cannot.
//
// To recompile the eBPF bytecode (requires Linux):
//
//go:generate make -C ../../../.. ebpf-build
package ebpf

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/cilium/ebpf/rlimit"
	kafka "github.com/segmentio/kafka-go"
)

// LatencyEvent mirrors the C struct latency_event.
type LatencyEvent struct {
	SrcIP     string  `json:"src_ip"`
	DstIP     string  `json:"dst_ip"`
	SrcPort   uint16  `json:"src_port"`
	DstPort   uint16  `json:"dst_port"`
	Proto     uint8   `json:"proto"`
	RTTNS     uint64  `json:"rtt_ns"`
	IngressNS uint64  `json:"ingress_ns"`
	EgressNS  uint64  `json:"egress_ns"`
	SessionID string  `json:"session_id"`
	SandboxID string  `json:"sandbox_id"`
}

// rawEvent mirrors the C struct latency_event (packed layout).
type rawEvent struct {
	Saddr     uint32
	Daddr     uint32
	Sport     uint16
	Dport     uint16
	Proto     uint8
	_pad      [3]uint8
	RTTNS     uint64
	IngressNS uint64
	EgressNS  uint64
}

// Prober attaches TC eBPF programs to a network interface and streams
// kernel-level latency events to a Kafka topic.
type Prober struct {
	iface     string
	sessionID string
	sandboxID string
	broker    string
	topic     string

	objs    TcLatencyObjects
	ingressLink link.Link
	egressLink  link.Link
	reader  *ringbuf.Reader
	writer  *kafka.Writer
}

// NewProber creates a Prober for the given network interface.
// broker should be the Redpanda/Kafka broker address, e.g. "redpanda:9092".
func NewProber(iface, sessionID, sandboxID, broker, topic string) (*Prober, error) {
	if err := rlimit.RemoveMemlock(); err != nil {
		return nil, fmt.Errorf("remove memlock: %w", err)
	}

	var objs TcLatencyObjects
	if err := LoadTcLatencyObjects(&objs, &ebpf.CollectionOptions{}); err != nil {
		return nil, fmt.Errorf("load eBPF objects: %w", err)
	}

	netIface, err := net.InterfaceByName(iface)
	if err != nil {
		objs.Close()
		return nil, fmt.Errorf("interface %q not found: %w", iface, err)
	}

	ingressLink, err := link.AttachTCX(link.TCXOptions{
		Interface: netIface.Index,
		Program:   objs.TcIngress,
		Attach:    ebpf.AttachTCXIngress,
	})
	if err != nil {
		objs.Close()
		return nil, fmt.Errorf("attach TC ingress: %w", err)
	}

	egressLink, err := link.AttachTCX(link.TCXOptions{
		Interface: netIface.Index,
		Program:   objs.TcEgress,
		Attach:    ebpf.AttachTCXEgress,
	})
	if err != nil {
		ingressLink.Close()
		objs.Close()
		return nil, fmt.Errorf("attach TC egress: %w", err)
	}

	reader, err := ringbuf.NewReader(objs.LatencyEvents)
	if err != nil {
		egressLink.Close()
		ingressLink.Close()
		objs.Close()
		return nil, fmt.Errorf("open ring buffer: %w", err)
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{broker},
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 5 * time.Millisecond,
	})

	return &Prober{
		iface:       iface,
		sessionID:   sessionID,
		sandboxID:   sandboxID,
		broker:      broker,
		topic:       topic,
		objs:        objs,
		ingressLink: ingressLink,
		egressLink:  egressLink,
		reader:      reader,
		writer:      writer,
	}, nil
}

// Run reads from the eBPF ring buffer and publishes events to Kafka until ctx
// is cancelled. Blocks until done.
func (p *Prober) Run(ctx context.Context) error {
	defer p.close()

	for {
		record, err := p.reader.Read()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return fmt.Errorf("ring buffer read: %w", err)
			}
		}

		ev, err := p.parseEvent(record.RawSample)
		if err != nil {
			continue
		}

		payload, err := json.Marshal(ev)
		if err != nil {
			continue
		}

		_ = p.writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(ev.SessionID + ":" + ev.SandboxID),
			Value: payload,
		})
	}
}

func (p *Prober) parseEvent(raw []byte) (LatencyEvent, error) {
	if len(raw) < 32 {
		return LatencyEvent{}, fmt.Errorf("short record: %d bytes", len(raw))
	}

	var r rawEvent
	// Manual decode — avoids unsafe.Pointer aliasing issues with packed C structs.
	r.Saddr = leUint32(raw[0:4])
	r.Daddr = leUint32(raw[4:8])
	r.Sport = leUint16(raw[8:10])
	r.Dport = leUint16(raw[10:12])
	r.Proto = raw[12]
	// [13:16] padding
	r.RTTNS     = leUint64(raw[16:24])
	r.IngressNS = leUint64(raw[24:32])
	r.EgressNS  = leUint64(raw[32:40])

	return LatencyEvent{
		SrcIP:     uint32ToIP(r.Saddr),
		DstIP:     uint32ToIP(r.Daddr),
		SrcPort:   r.Sport,
		DstPort:   r.Dport,
		Proto:     r.Proto,
		RTTNS:     r.RTTNS,
		IngressNS: r.IngressNS,
		EgressNS:  r.EgressNS,
		SessionID: p.sessionID,
		SandboxID: p.sandboxID,
	}, nil
}

func (p *Prober) close() {
	p.reader.Close()
	p.egressLink.Close()
	p.ingressLink.Close()
	p.objs.Close()
	_ = p.writer.Close()
}

func leUint16(b []byte) uint16 { return uint16(b[0]) | uint16(b[1])<<8 }
func leUint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
func leUint64(b []byte) uint64 {
	return uint64(leUint32(b[0:4])) | uint64(leUint32(b[4:8]))<<32
}
func uint32ToIP(n uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d", n&0xff, (n>>8)&0xff, (n>>16)&0xff, (n>>24)&0xff)
}
