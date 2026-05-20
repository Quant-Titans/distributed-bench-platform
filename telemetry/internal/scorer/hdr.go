// Package scorer computes composite benchmark scores using HDR histograms.
package scorer

import (
	"sync"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

const (
	// HDR histogram range: 1ns to 10 seconds, 3 significant figures.
	minLatencyNS int64 = 1
	maxLatencyNS int64 = 10_000_000_000
	sigFigs            = 3
)

// LatencyHistogram wraps two HDR histograms: app-layer and kernel-layer RTT.
type LatencyHistogram struct {
	mu     sync.Mutex
	app    *hdrhistogram.Histogram
	kernel *hdrhistogram.Histogram
}

func NewLatencyHistogram() *LatencyHistogram {
	return &LatencyHistogram{
		app:    hdrhistogram.New(minLatencyNS, maxLatencyNS, sigFigs),
		kernel: hdrhistogram.New(minLatencyNS, maxLatencyNS, sigFigs),
	}
}

func (h *LatencyHistogram) RecordApp(ns int64) {
	if ns <= 0 {
		return
	}
	h.mu.Lock()
	_ = h.app.RecordValue(ns)
	h.mu.Unlock()
}

func (h *LatencyHistogram) RecordKernel(ns int64) {
	if ns <= 0 {
		return
	}
	h.mu.Lock()
	_ = h.kernel.RecordValue(ns)
	h.mu.Unlock()
}

// AppPercentiles returns p50/p90/p99/p999 for app-layer RTT.
func (h *LatencyHistogram) AppPercentiles() (p50, p90, p99, p999 float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return float64(h.app.ValueAtQuantile(50)),
		float64(h.app.ValueAtQuantile(90)),
		float64(h.app.ValueAtQuantile(99)),
		float64(h.app.ValueAtQuantile(99.9))
}

// KernelPercentiles returns p50/p90/p99/p999 for kernel-layer RTT (eBPF).
func (h *LatencyHistogram) KernelPercentiles() (p50, p90, p99, p999 float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.kernel.TotalCount() == 0 {
		// Fall back to app-layer if eBPF not available (non-Linux)
		return float64(h.app.ValueAtQuantile(50)),
			float64(h.app.ValueAtQuantile(90)),
			float64(h.app.ValueAtQuantile(99)),
			float64(h.app.ValueAtQuantile(99.9))
	}
	return float64(h.kernel.ValueAtQuantile(50)),
		float64(h.kernel.ValueAtQuantile(90)),
		float64(h.kernel.ValueAtQuantile(99)),
		float64(h.kernel.ValueAtQuantile(99.9))
}

func (h *LatencyHistogram) TotalCount() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.app.TotalCount()
}

func (h *LatencyHistogram) Reset() {
	h.mu.Lock()
	h.app.Reset()
	h.kernel.Reset()
	h.mu.Unlock()
}
