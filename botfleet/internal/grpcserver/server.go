package grpcserver

import (
	"context"
	"fmt"
	"sync"

	"github.com/Quant-Titans/distributed-bench-platform/botfleet/internal/worker"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SpawnFleetRequest mirrors the proto message (avoids direct proto dependency
// inside this package for flexibility; the handler layer translates).
type SpawnFleetRequest struct {
	SessionID   string
	EndpointURL string
	FIXEndpoint string // TCP addr for FIX 4.4 acceptor (optional)
	Symbol      string
	BotCount    int32
	TargetTPS   int32
	DurationSec int64
	Mix         worker.ArchetypeMix
}

type SpawnFleetResponse struct {
	FleetID string
}

type StopFleetRequest struct {
	FleetID string
}

// Server manages active fleets and exposes a gRPC API.
type Server struct {
	baseCfg worker.FleetConfig
	mu      sync.Mutex
	fleets  map[string]*activeFleet
}

type activeFleet struct {
	fleet  *worker.Fleet
	cancel context.CancelFunc
}

func New(baseCfg worker.FleetConfig) *Server {
	return &Server{
		baseCfg: baseCfg,
		fleets:  make(map[string]*activeFleet),
	}
}

// Register attaches this server to a gRPC server — real proto bindings wired
// in main.go after proto generation.
func (s *Server) Register(grpcSrv *grpc.Server) {
	// Registration happens via generated proto code in main.go.
	// This package is kept proto-free so it compiles before protoc runs.
	_ = grpcSrv
}

func (s *Server) SpawnFleet(ctx context.Context, req SpawnFleetRequest) (*SpawnFleetResponse, error) {
	fleetID := fmt.Sprintf("fleet-%s", uuid.New().String()[:8])

	cfg := s.baseCfg
	cfg.FleetID = fleetID
	cfg.SessionID = req.SessionID
	cfg.Symbol = req.Symbol
	cfg.EndpointURL = req.EndpointURL
	cfg.FIXEndpoint = req.FIXEndpoint
	cfg.TargetTPS = int(req.TargetTPS)
	cfg.DurationSec = req.DurationSec
	cfg.Mix = req.Mix

	if cfg.Mix == (worker.ArchetypeMix{}) {
		// Default 1000-bot mix when caller does not specify per-archetype counts.
		// Percentages: 25% MM, 20% Momentum, 30% Noise, 10% Slicer, 10% LatArb, 5% FIX
		total := int(req.BotCount)
		if total <= 0 {
			total = 1000
		}
		cfg.Mix = worker.ArchetypeMix{
			MarketMakers:         total * 25 / 100,
			MomentumTraders:      total * 20 / 100,
			NoiseTraders:         total * 30 / 100,
			InstitutionalSlicers: total * 10 / 100,
			LatencyArbs:          total * 10 / 100,
			FIXNoise:             total * 5 / 100,
		}
	}

	fleet := worker.NewFleet(cfg)
	fleetCtx, cancel := context.WithCancel(context.Background())

	s.mu.Lock()
	s.fleets[fleetID] = &activeFleet{fleet: fleet, cancel: cancel}
	s.mu.Unlock()

	go func() {
		_ = fleet.Run(fleetCtx)
		s.mu.Lock()
		delete(s.fleets, fleetID)
		s.mu.Unlock()
	}()

	return &SpawnFleetResponse{FleetID: fleetID}, nil
}

func (s *Server) StopFleet(_ context.Context, req StopFleetRequest) error {
	s.mu.Lock()
	af, ok := s.fleets[req.FleetID]
	if ok {
		af.cancel()
		delete(s.fleets, req.FleetID)
	}
	s.mu.Unlock()

	if !ok {
		return status.Errorf(codes.NotFound, "fleet %s not found", req.FleetID)
	}
	return nil
}

func (s *Server) FleetStatus(_ context.Context, fleetID string) (int32, int64, error) {
	s.mu.Lock()
	af, ok := s.fleets[fleetID]
	s.mu.Unlock()

	if !ok {
		return 0, 0, status.Errorf(codes.NotFound, "fleet %s not found", fleetID)
	}
	return af.fleet.ActiveBots(), af.fleet.OrdersSent(), nil
}
