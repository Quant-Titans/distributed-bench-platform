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
)

const (
	sandboxNetwork  = "sandbox_net"
	seccompPath     = "/etc/sandbox/seccomp.json"
	defaultMemoryMB = 512
	defaultCPUCores = "0"
	defaultTimeoutS = 120
)

type Config struct {
	Image     string   `json:"image"`
	CPUCores  string   `json:"cpu_cores"`
	MemoryMB  int64    `json:"memory_mb"`
	TimeoutS  int      `json:"timeout_s"`
	Env       []string `json:"env,omitempty"`
}

type Info struct {
	ID        string    `json:"id"`
	Image     string    `json:"image"`
	Endpoint  string    `json:"endpoint"`
	Status    string    `json:"status"`
	StartedAt time.Time `json:"started_at"`
	cancel    context.CancelFunc
}

type Manager struct {
	cli         *client.Client
	seccompJSON string
	mu          sync.RWMutex
	sandboxes   map[string]*Info
}

func New() (*Manager, error) {
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
		sandboxes:   make(map[string]*Info),
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
			"quant-titans.role":    "sandbox",
			"quant-titans.managed": "true",
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

	ip := ""
	if net, ok := inspect.NetworkSettings.Networks[sandboxNetwork]; ok {
		ip = net.IPAddress
	}

	runCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutS)*time.Second)

	shortID := resp.ID[:12]
	info := &Info{
		ID:        shortID,
		Image:     cfg.Image,
		Endpoint:  fmt.Sprintf("http://%s:8080", ip),
		Status:    "running",
		StartedAt: time.Now(),
		cancel:    cancel,
	}

	m.mu.Lock()
	m.sandboxes[shortID] = info
	m.mu.Unlock()

	go m.enforceTimeout(runCtx, resp.ID, shortID)

	return info, nil
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
		delete(m.sandboxes, shortID)
	}
	m.mu.Unlock()

	timeout := 2
	bgCtx := context.Background()
	_ = m.cli.ContainerStop(bgCtx, containerID, container.StopOptions{Timeout: &timeout})
	_ = m.cli.ContainerRemove(bgCtx, containerID, container.RemoveOptions{Force: true})
}
