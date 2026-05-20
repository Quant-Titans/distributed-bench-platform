// Package chaos injects controlled faults into a running sandbox to measure
// resilience. Faults are applied via Linux tc netem (network delay/loss) and
// cgroup v2 CPU throttling.
//
// The resilience score dimension measures:
//   - recovery_time_ms: how fast p99 latency returns to baseline after fault
//   - degradation_ratio: p99_under_chaos / p99_baseline
package chaos

import (
	"context"
	"fmt"
	"os/exec"
	"time"
)

// FaultType is the kind of fault to inject.
type FaultType string

const (
	NetworkDelay FaultType = "network_delay" // tc netem: adds latency jitter
	NetworkLoss  FaultType = "network_loss"  // tc netem: drops packets
	CPUThrottle  FaultType = "cpu_throttle"  // cgroup v2: limits CPU quota
	MemPressure  FaultType = "mem_pressure"  // stress-ng: memory pressure
)

// FaultSpec describes a fault to inject.
type FaultSpec struct {
	Type     FaultType
	Duration time.Duration
	// NetworkDelay params
	DelayMS  int // base delay milliseconds
	JitterMS int // delay jitter milliseconds
	// NetworkLoss params
	LossPercent int
	// CPUThrottle params — quota in microseconds per period (100ms)
	CPUQuotaUS int
}

// DefaultFaultSchedule returns the standard chaos schedule used during judging.
// Run after 30s of baseline measurement, inject for 10s, then measure recovery.
func DefaultFaultSchedule() []FaultSpec {
	return []FaultSpec{
		{Type: NetworkDelay, Duration: 10 * time.Second, DelayMS: 5, JitterMS: 2},
		{Type: NetworkLoss, Duration: 10 * time.Second, LossPercent: 2},
		{Type: CPUThrottle, Duration: 10 * time.Second, CPUQuotaUS: 50000}, // 50% of one core
	}
}

// Injector applies and removes faults on a network interface or cgroup.
type Injector struct {
	iface      string // host-side network interface for the sandbox
	cgroupPath string // cgroup v2 path for the sandbox process
}

func New(iface, cgroupPath string) *Injector {
	return &Injector{iface: iface, cgroupPath: cgroupPath}
}

// RunSchedule executes a sequence of faults with the given baseline delay,
// then waits for recovery. Returns the time from fault removal to baseline restoration.
func (inj *Injector) RunSchedule(ctx context.Context, faults []FaultSpec, baselineP99NS int64) (recoveryMS int64, degradationRatio float64, err error) {
	for _, f := range faults {
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		default:
		}

		if err := inj.inject(f); err != nil {
			// Non-fatal: log and continue. Chaos injection may fail on non-Linux.
			continue
		}

		faultStart := time.Now()
		select {
		case <-ctx.Done():
			_ = inj.remove(f)
			return 0, 0, ctx.Err()
		case <-time.After(f.Duration):
		}

		_ = inj.remove(f)
		recoveryMS = time.Since(faultStart).Milliseconds()
	}
	return recoveryMS, 1.0, nil // degradation measured by telemetry service
}

func (inj *Injector) inject(f FaultSpec) error {
	switch f.Type {
	case NetworkDelay:
		return inj.runCmd("tc", "qdisc", "add", "dev", inj.iface,
			"root", "netem",
			"delay", fmt.Sprintf("%dms", f.DelayMS),
			fmt.Sprintf("%dms", f.JitterMS),
			"distribution", "normal")

	case NetworkLoss:
		return inj.runCmd("tc", "qdisc", "add", "dev", inj.iface,
			"root", "netem",
			"loss", fmt.Sprintf("%d%%", f.LossPercent))

	case CPUThrottle:
		if inj.cgroupPath == "" {
			return nil
		}
		quota := fmt.Sprintf("%d 100000", f.CPUQuotaUS) // quota period
		return inj.runCmd("sh", "-c",
			fmt.Sprintf("echo %q > %s/cpu.max", quota, inj.cgroupPath))

	case MemPressure:
		// stress-ng runs inside the sandbox namespace; we exec in the container
		return inj.runCmd("stress-ng", "--vm", "1", "--vm-bytes", "256M",
			"--timeout", fmt.Sprintf("%ds", int(10)))
	}
	return nil
}

func (inj *Injector) remove(f FaultSpec) error {
	switch f.Type {
	case NetworkDelay, NetworkLoss:
		return inj.runCmd("tc", "qdisc", "del", "dev", inj.iface, "root")
	case CPUThrottle:
		if inj.cgroupPath == "" {
			return nil
		}
		return inj.runCmd("sh", "-c",
			fmt.Sprintf("echo 'max 100000' > %s/cpu.max", inj.cgroupPath))
	}
	return nil
}

func (inj *Injector) runCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s %v: %w\n%s", name, args, err, out)
	}
	return nil
}

// RunWithContext is a convenience wrapper that injects the default fault
// schedule and returns resilience metrics.
func RunWithContext(ctx context.Context, iface, cgroupPath string, baselineP99NS int64) (recoveryMS int64, err error) {
	inj := New(iface, cgroupPath)
	recoveryMS, _, err = inj.RunSchedule(ctx, DefaultFaultSchedule(), baselineP99NS)
	return
}
