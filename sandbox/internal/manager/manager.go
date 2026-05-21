package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/strslice"
	"github.com/docker/docker/client"
	kafka "github.com/segmentio/kafka-go"

	sandboxchaos "github.com/Quant-Titans/distributed-bench-platform/sandbox/internal/chaos"
	sandboxebpf "github.com/Quant-Titans/distributed-bench-platform/sandbox/internal/ebpf"
)

const (
	seccompPath     = "/etc/sandbox/seccomp.json"
	defaultMemoryMB = 512
	defaultCPUCores = "0"
	defaultTimeoutS = 120
	chaosBaselineS  = 30 // seconds of clean load before injecting faults
)

// KafkaConfig holds Redpanda/Kafka connection settings.
type KafkaConfig struct {
	Broker      string // e.g. "redpanda:9092"
	EBPFTopic   string // e.g. "telemetry.kernel_latency"
	EventsTopic string // e.g. "bench.events" — chaos lifecycle events
}

type Config struct {
	Image        string   `json:"image"`
	CPUCores     string   `json:"cpu_cores"`
	MemoryMB     int64    `json:"memory_mb"`
	TimeoutS     int      `json:"timeout_s"`
	Env          []string `json:"env,omitempty"`
	SessionID    string   `json:"session_id"`
	TeamName     string   `json:"team_name,omitempty"`
	ChaosEnabled bool     `json:"chaos_enabled"`
}

type Info struct {
	ID           string    `json:"id"`
	SessionID    string    `json:"session_id"`
	TeamName     string    `json:"team_name,omitempty"`
	Image        string    `json:"image"`
	Endpoint     string    `json:"endpoint"`
	ContainerIP  string    `json:"container_ip"`
	Status       string    `json:"status"`
	StartedAt    time.Time `json:"started_at"`
	EBPFIface    string    `json:"ebpf_iface,omitempty"`
	ChaosEnabled bool      `json:"chaos_enabled"`
	cancel       context.CancelFunc
	proberStop   context.CancelFunc
	chaosStop    context.CancelFunc
}

// chaosLifecycleEvent is published to bench.events when chaos starts or ends.
type chaosLifecycleEvent struct {
	SessionID   string `json:"session_id"`
	SandboxID   string `json:"sandbox_id"`
	EventType   string `json:"event_type"` // "chaos_start" | "chaos_end"
	FaultType   string `json:"fault_type"`
	TimestampNS int64  `json:"timestamp_ns"`
}

type Manager struct {
	cli            *client.Client
	seccompJSON    string
	kafka          KafkaConfig
	eventsWriter   *kafka.Writer
	sandboxNetwork string
	insecure       bool
	mu             sync.RWMutex
	sandboxes      map[string]*Info
	ipRegistry     map[string]string
}

func New(kafkaCfg KafkaConfig) (*Manager, error) {
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

	var eventsWriter *kafka.Writer
	if kafkaCfg.Broker != "" && kafkaCfg.EventsTopic != "" {
		eventsWriter = kafka.NewWriter(kafka.WriterConfig{
			Brokers:      []string{kafkaCfg.Broker},
			Topic:        kafkaCfg.EventsTopic,
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 100 * time.Millisecond,
		})
	}

	sandboxNet := os.Getenv("SANDBOX_NETWORK")
	if sandboxNet == "" {
		sandboxNet = "sandbox_net"
	}
	insecure := os.Getenv("SANDBOX_INSECURE") == "true"

	m := &Manager{
		cli:            cli,
		seccompJSON:    string(raw),
		kafka:          kafkaCfg,
		eventsWriter:   eventsWriter,
		sandboxNetwork: sandboxNet,
		insecure:       insecure,
		sandboxes:      make(map[string]*Info),
		ipRegistry:     make(map[string]string),
	}

	if !insecure {
		if err := m.ensureNetwork(context.Background()); err != nil {
			cli.Close()
			return nil, fmt.Errorf("setup sandbox network: %w", err)
		}
	}

	return m, nil
}

func (m *Manager) Close() {
	if m.eventsWriter != nil {
		_ = m.eventsWriter.Close()
	}
	m.cli.Close()
}

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

	// SANDBOX_INSECURE=true: strip all OCI security constraints so contestant
	// containers start on GitHub Actions runners (which reject custom seccomp +
	// host-network + CapDrop combinations). In production this env var is unset
	// and all isolation layers are active.
	insecure := m.insecure

	// host network mode shares the existing host netns — no new namespace and
	// no bind-mount. Required on CI runners where the Docker daemon cannot
	// bind-mount netns for API-created containers.
	useHostNet := m.sandboxNetwork == "host"

	networkMode := container.NetworkMode(m.sandboxNetwork)

	hostCfg := &container.HostConfig{
		Resources: container.Resources{
			Memory:     cfg.MemoryMB * 1024 * 1024,
			CpusetCpus: cfg.CPUCores,
		},
		NetworkMode: networkMode,
	}

	if !insecure {
		hostCfg.SecurityOpt = []string{
			"no-new-privileges:true",
			"seccomp=" + m.seccompJSON,
		}
		hostCfg.ReadonlyRootfs = true
		hostCfg.Tmpfs = map[string]string{
			"/tmp": "rw,noexec,nosuid,size=64m",
		}
		hostCfg.CapDrop = strslice.StrSlice{"ALL"}
		hostCfg.CapAdd = strslice.StrSlice{"NET_BIND_SERVICE"}
	}

	// For host-network mode, find a free port on the host and pass it to the
	// contestant binary via LISTEN_ADDR. Named networks use the container IP.
	containerPort := 8080
	if useHostNet {
		if p, err := freePort(); err == nil {
			containerPort = p
		}
	}

	env := append(cfg.Env, fmt.Sprintf("LISTEN_ADDR=:%d", containerPort))

	containerCfg := &container.Config{
		Image: cfg.Image,
		Env:   env,
		Labels: map[string]string{
			"quant-titans.role":    "sandbox",
			"quant-titans.managed": "true",
			"quant-titans.session": cfg.SessionID,
		},
	}

	// host mode must not specify endpoint config.
	var netCfg *network.NetworkingConfig
	if !useHostNet {
		netCfg = &network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{
				m.sandboxNetwork: {},
			},
		}
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

	containerIP := "127.0.0.1" // default for host-network mode
	if !useHostNet {
		if n, ok := inspect.NetworkSettings.Networks[m.sandboxNetwork]; ok {
			containerIP = n.IPAddress
		}
	}

	bridgeIface, _ := m.resolveBridgeIface(ctx)

	runCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutS)*time.Second)
	proberCtx, proberCancel := context.WithCancel(context.Background())
	chaosCtx, chaosCancel := context.WithCancel(context.Background())

	shortID := resp.ID[:12]
	info := &Info{
		ID:           shortID,
		SessionID:    cfg.SessionID,
		TeamName:     cfg.TeamName,
		Image:        cfg.Image,
		Endpoint:     fmt.Sprintf("http://%s:%d", containerIP, containerPort),
		ContainerIP:  containerIP,
		Status:       "running",
		StartedAt:    time.Now(),
		EBPFIface:    bridgeIface,
		ChaosEnabled: cfg.ChaosEnabled,
		cancel:       cancel,
		proberStop:   proberCancel,
		chaosStop:    chaosCancel,
	}

	m.mu.Lock()
	m.sandboxes[shortID] = info
	if containerIP != "" {
		m.ipRegistry[containerIP] = shortID
	}
	m.mu.Unlock()

	if bridgeIface != "" && m.kafka.Broker != "" {
		go m.runEBPFProber(proberCtx, bridgeIface, cfg.SessionID, shortID)
	}

	// Chaos injection: wait for baseline, then run fault schedule.
	if cfg.ChaosEnabled && bridgeIface != "" {
		go m.runChaos(chaosCtx, info, bridgeIface)
	} else {
		chaosCancel() // not needed; release immediately
	}

	go m.enforceTimeout(runCtx, resp.ID, shortID)

	return info, nil
}

// runChaos waits for the baseline measurement window, then runs the default
// fault schedule, publishing chaos_start and chaos_end events to bench.events.
func (m *Manager) runChaos(ctx context.Context, info *Info, iface string) {
	// Wait for baseline measurement window before injecting any faults.
	select {
	case <-ctx.Done():
		return
	case <-time.After(chaosBaselineS * time.Second):
	}

	log.Printf("chaos: starting fault schedule for session %s (iface=%s)", info.SessionID, iface)

	m.publishChaosEvent(ctx, chaosLifecycleEvent{
		SessionID:   info.SessionID,
		SandboxID:   info.ID,
		EventType:   "chaos_start",
		FaultType:   "schedule",
		TimestampNS: time.Now().UnixNano(),
	})

	// cgroup v2 path for CPU throttling — valid on systemd Linux hosts.
	cgroupPath := fmt.Sprintf("/sys/fs/cgroup/system.slice/docker-%s.scope", info.ID)
	inj := sandboxchaos.New(iface, cgroupPath)

	if _, _, err := inj.RunSchedule(ctx, sandboxchaos.DefaultFaultSchedule(), 0); err != nil {
		log.Printf("chaos: schedule error for session %s: %v", info.SessionID, err)
	}

	m.publishChaosEvent(ctx, chaosLifecycleEvent{
		SessionID:   info.SessionID,
		SandboxID:   info.ID,
		EventType:   "chaos_end",
		FaultType:   "schedule",
		TimestampNS: time.Now().UnixNano(),
	})

	log.Printf("chaos: completed fault schedule for session %s", info.SessionID)
}

func (m *Manager) publishChaosEvent(ctx context.Context, ev chaosLifecycleEvent) {
	if m.eventsWriter == nil {
		return
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	if err := m.eventsWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(ev.SessionID),
		Value: b,
	}); err != nil {
		log.Printf("chaos: publish event %s: %v", ev.EventType, err)
	}
}

func (m *Manager) resolveBridgeIface(ctx context.Context) (string, error) {
	nets, err := m.cli.NetworkList(ctx, types.NetworkListOptions{})
	if err != nil {
		return "", err
	}
	for _, n := range nets {
		if n.Name == m.sandboxNetwork {
			return fmt.Sprintf("br-%s", n.ID[:12]), nil
		}
	}
	return "", fmt.Errorf("network %s not found", m.sandboxNetwork)
}

func (m *Manager) runEBPFProber(ctx context.Context, iface, sessionID, sandboxID string) {
	prober, err := sandboxebpf.NewProber(iface, sessionID, sandboxID, m.kafka.Broker, m.kafka.EBPFTopic)
	if err != nil {
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
		if info.chaosStop != nil {
			info.chaosStop()
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
		if n.Name == m.sandboxNetwork {
			return nil // already exists (either sandbox_net or a compose network)
		}
	}
	// Only create if it's our own managed network; never try to create bridge/host/none.
	if m.sandboxNetwork == "bridge" || m.sandboxNetwork == "host" || m.sandboxNetwork == "none" {
		return nil
	}
	_, err = m.cli.NetworkCreate(ctx, m.sandboxNetwork, types.NetworkCreate{
		Driver: "bridge",
		Options: map[string]string{
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
		if info.chaosStop != nil {
			info.chaosStop()
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

// freePort asks the OS for an available TCP port by binding to :0.
func freePort() (int, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port, nil
}
