//go:build linux

// Hand-written equivalents of what bpf2go would generate from tc_latency.c.
// Regenerate via: make ebpf-build  (compiles in a Linux Docker container).
package ebpf

import (
	"fmt"

	"github.com/cilium/ebpf"
)

// TcLatencyObjects contains all maps and programs after loading.
type TcLatencyObjects struct {
	TcLatencyPrograms
	TcLatencyMaps
}

func (o *TcLatencyObjects) Close() {
	o.TcLatencyPrograms.Close()
	o.TcLatencyMaps.Close()
}

// TcLatencyPrograms holds the compiled TC programs.
type TcLatencyPrograms struct {
	TcIngress *ebpf.Program `ebpf:"tc_ingress"`
	TcEgress  *ebpf.Program `ebpf:"tc_egress"`
}

func (p *TcLatencyPrograms) Close() {
	if p.TcIngress != nil {
		p.TcIngress.Close()
	}
	if p.TcEgress != nil {
		p.TcEgress.Close()
	}
}

// TcLatencyMaps holds the eBPF maps.
type TcLatencyMaps struct {
	IngressTs     *ebpf.Map `ebpf:"ingress_ts"`
	LatencyEvents *ebpf.Map `ebpf:"latency_events"`
}

func (m *TcLatencyMaps) Close() {
	if m.IngressTs != nil {
		m.IngressTs.Close()
	}
	if m.LatencyEvents != nil {
		m.LatencyEvents.Close()
	}
}

// LoadTcLatencyObjects loads the compiled eBPF bytecode and populates obj.
// The .o file is compiled from bpf/tc_latency.c via `make ebpf-build`.
func LoadTcLatencyObjects(obj *TcLatencyObjects, opts *ebpf.CollectionOptions) error {
	spec, err := loadTcLatencySpec()
	if err != nil {
		return fmt.Errorf("load spec: %w", err)
	}
	return spec.LoadAndAssign(obj, opts)
}
