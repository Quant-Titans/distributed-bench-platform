package manager

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/strslice"
	"github.com/docker/docker/client"

	sandboxebpf "github.com/Quant-Titans/distributed-bench-platform/sandbox/internal/ebpf"
)

const (
	sandboxNetwork  = "sandbox_net"
	seccompPath     = "/etc/sandbox/seccomp.json"
	defaultMemoryMB = 512
	defaultCPUCores = "0"
	defaultTimeoutS = 120
)

// KafkaConfig holds Redpanda/Kafka connection settings for eBPF event publishing.
type KafkaConfig struct {
	Broker     string // e.g. "redpanda:9092"
	EBPFTopic  string // e.g. "telemetry.kernel_latency"
}

type Config struct {
	Image     string   `json:"image"`
	CPUCores  string   `json:"cpu_cores"`
	MemoryMB  int64    `json:"memory_mb"`
	TimeoutS  int      `json:"timeout_s"`
	Env       []string `json:"env,omitempty"`
	SessionID string   `json:"session_id"`
	TeamName  string   `json:"team_name,omitempty"`
}

type Info struct {
	ID          string    `json:"id"`
	SessionID   string    `json:"session_id"`
	TeamName    string    `json:"team_name,omitempty"`
	Image       string    `json:"image"`
	Endpoint    string    `json:"endpoint"`
	ContainerIP string    `json:"container_ip"`
	Status      string    `json:"status"`
	StartedAt   time.Time `json:"started_at"`
	EBPFIface   string    `json:"ebpf_iface,omitempty"`
	cancel      context.CancelFunc
	proberStop  context.CancelFunc
}

type Manager struct {
	cli         *client.Client
	seccompJSON string
	kafka       KafkaConfig
	mu          sync.RWMutex
	sandboxes   map[string]*Info
	// ipRegistry maps containerIP → sandbox shortID for eBPF event annotation.
	ipRegistry  map[string]string
}

func New(kafka KafkaConfig) (*Manager, error) {
	cli, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}

	raw, err := os.ReadFile(seccompPath)
	if err != nil {
		return nil, fmt.Errorf("read seccomp profile at %s: %w", seccompPath, err)
	}

	m := &Manager{
		cli:         cli,
		seccompJSON: string(raw),
		kafka:       kafka,
		sandboxes:   make(map[string]*Info),
		ipRegistry:  make(map[string]string),
	}

	if err := m.ensureNetwork(context.Background()); err != nil {
		cli.Close()
		return nil, fmt.Errorf("setup sandbox network: %w", err)
	}

	return m, nil
}

func (m *Manager) Close() { m.cli.Close() }

func (m *Manager) Run(ctx context.Context, cfg Config) (*Info, error) {
	if cfg.MemoryMB == 0 {
		cfg.MemoryMB = defaultMemoryMB
	}
	if cfg.CPUCores == "" {
		cfg.CPUCores = defaultCPUCores
	}
	if cfg.TimeoutS == 0 {
		cfg.TimeoutS = defaultTimeoutS
	}

	hostCfg := &container.HostConfig{
		Resources: container.Resources{
			Memory:     cfg.MemoryMB * 1024 * 1024,
			CpusetCpus: cfg.CPUCores,
		},
		SecurityOpt: []string{
			"no-new-privileges:true",
			"seccomp=" + m.seccompJSON,
		},
		ReadonlyRootfs: true,
		Tmpfs: map[string]string{
			"/tmp": "rw,noexec,nosuid,size=64m",
		},
		CapDrop:     strslice.StrSlice{"ALL"},
		CapAdd:      strslice.StrSlice{"NET_BIND_SERVICE"},
		NetworkMode: container.NetworkMode(sandboxNetwork),
	}

	containerCfg := &container.Config{
		Image: cfg.Image,
		Env:   cfg.Env,
		Labels: map[string]string{
			"quant-titans.role":      "sandbox",
			"quant-titans.managed":   "true",
			"quant-titans.session":   cfg.SessionID,
		},
	}

	netCfg := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			sandboxNetwork: {},
		},
	}

	resp, err := m.cli.ContainerCreate(ctx, containerCfg, hostCfg, netCfg, nil, "")
	if err != nil {
		return nil, fmt.Errorf("container create: %w", err)
	}

	if err := m.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		_ = m.cli.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})
		return nil, fmt.Errorf("container start: %w", err)
	}

	inspect, err := m.cli.ContainerInspect(ctx, resp.ID)
	if err != nil {
		return nil, fmt.Errorf("container inspect: %w", err)
	}

	containerIP := ""
	if net, ok := inspect.NetworkSettings.Networks[sandboxNetwork]; ok {
		containerIP = net.IPAddress
	}

	bridgeIface, _ := m.resolveBridgeIface(ctx)

	runCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutS)*time.Second)
	proberCtx, proberCancel := context.WithCancel(context.Background())

	shortID := resp.ID[:12]
	info := &Info{
		ID:          shortID,
		SessionID:   cfg.SessionID,
		TeamName:    cfg.TeamName,
		Image:       cfg.Image,
		Endpoint:    fmt.Sprintf("http://%s:8080", containerIP),
		ContainerIP: containerIP,
		Status:      "running",
		StartedAt:   time.Now(),
		EBPFIface:   bridgeIface,
		cancel:      cancel,
		proberStop:  proberCancel,
	}

	m.mu.Lock()
	m.sandboxes[shortID] = info
	if containerIP != "" {
		m.ipRegistry[containerIP] = shortID
	}
	m.mu.Unlock()

	// Attach eBPF TC hook to the bridge interface — kernel-level RTT measurement.
	if bridgeIface != "" && m.kafka.Broker != "" {
		go m.runEBPFProber(proberCtx, bridgeIface, cfg.SessionID, shortID)
	}

	go m.enforceTimeout(runCtx, resp.ID, shortID)

	return info, nil
}

// resolveBridgeIface returns the host bridge interface name for sandbox_net.
// Docker names custom network bridges "br-<network_id[:12]>".
func (m *Manager) resolveBridgeIface(ctx context.Context) (string, error) {
	nets, err := m.cli.NetworkList(ctx, types.NetworkListOptions{})
	if err != nil {
		return "", err
	}
	for _, n := range nets {
		if n.Name == sandboxNetwork {
			return fmt.Sprintf("br-%s", n.ID[:12]), nil
		}
	}
	return "", fmt.Errorf("network %s not found", sandboxNetwork)
}

func (m *Manager) runEBPFProber(ctx context.Context, iface, sessionID, sandboxID string) {
	prober, err := sandboxebpf.NewProber(iface, sessionID, sandboxID, m.kafka.Broker, m.kafka.EBPFTopic)
	if err != nil {
		// eBPF unavailable (non-Linux, missing caps) — degrade gracefully.
		return
	}
	_ = prober.Run(ctx)
}

func (m *Manager) Status(id string) (*Info, error) {
	m.mu.RLock()
	_, ok := m.sandboxes[id]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("sandbox %s not found", id)
	}

	inspect, err := m.cli.ContainerInspect(context.Background(), id)
	if err != nil {
		return nil, fmt.Errorf("container inspect: %w", err)
	}

	m.mu.Lock()
	info := m.sandboxes[id]
	if info != nil {
		info.Status = inspect.State.Status
	}
	m.mu.Unlock()

	return info, nil
}

func (m *Manager) Stop(id string) error {
	m.mu.Lock()
	info, ok := m.sandboxes[id]
	if ok {
		info.cancel()
		if info.proberStop != nil {
			info.proberStop()
		}
		if info.ContainerIP != "" {
			delete(m.ipRegistry, info.ContainerIP)
		}
		delete(m.sandboxes, id)
	}
	m.mu.Unlock()

	timeout := 5
	if err := m.cli.ContainerStop(context.Background(), id, container.StopOptions{Timeout: &timeout}); err != nil {
		return fmt.Errorf("container stop: %w", err)
	}
	return m.cli.ContainerRemove(context.Background(), id, container.RemoveOptions{Force: true})
}

func (m *Manager) ensureNetwork(ctx context.Context) error {
	nets, err := m.cli.NetworkList(ctx, types.NetworkListOptions{})
	if err != nil {
		return err
	}
	for _, n := range nets {
		if n.Name == sandboxNetwork {
			return nil
		}
	}
	_, err = m.cli.NetworkCreate(ctx, sandboxNetwork, types.NetworkCreate{
		Driver: "bridge",
		Options: map[string]string{
			// prevent sandbox containers from reaching each other
			"com.docker.network.bridge.enable_icc":           "false",
			"com.docker.network.bridge.enable_ip_masquerade": "true",
		},
	})
	return err
}

func (m *Manager) enforceTimeout(ctx context.Context, containerID, shortID string) {
	<-ctx.Done()

	m.mu.Lock()
	if info, ok := m.sandboxes[shortID]; ok {
		info.Status = "timed_out"
		if info.proberStop != nil {
			info.proberStop()
		}
		if info.ContainerIP != "" {
			delete(m.ipRegistry, info.ContainerIP)
		}
		delete(m.sandboxes, shortID)
	}
	m.mu.Unlock()

	timeout := 2
	bgCtx := context.Background()
	_ = m.cli.ContainerStop(bgCtx, containerID, container.StopOptions{Timeout: &timeout})
	_ = m.cli.ContainerRemove(bgCtx, containerID, container.RemoveOptions{Force: true})
}
