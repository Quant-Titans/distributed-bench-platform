//go:build !linux

// Stub — eBPF TC hooks require Linux. This file satisfies the compiler on macOS
// (CI, local dev). The real implementation is in prober.go (linux build tag).
package ebpf

import "context"

// LatencyEvent mirrors the C struct latency_event.
type LatencyEvent struct {
	SrcIP     string `json:"src_ip"`
	DstIP     string `json:"dst_ip"`
	SrcPort   uint16 `json:"src_port"`
	DstPort   uint16 `json:"dst_port"`
	Proto     uint8  `json:"proto"`
	RTTNS     uint64 `json:"rtt_ns"`
	IngressNS uint64 `json:"ingress_ns"`
	EgressNS  uint64 `json:"egress_ns"`
	SessionID string `json:"session_id"`
	SandboxID string `json:"sandbox_id"`
}

// Prober is a no-op on non-Linux platforms.
type Prober struct{}

func NewProber(iface, sessionID, sandboxID, broker, topic string) (*Prober, error) {
	return &Prober{}, nil
}

func (p *Prober) Run(_ context.Context) error { return nil }
