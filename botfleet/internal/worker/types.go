package worker

import (
	"context"
	"net/http"
	"time"
)

// Archetype identifies the trading strategy a bot uses.
type Archetype int

const (
	NoiseTrader        Archetype = iota // random walk orders
	MarketMaker                         // Avellaneda-Stoikov spread quoting
	Momentum                            // trend-following
	InstitutionalSlicer                 // iceberg large-order slicer
	LatencyArb                          // adversarial speed arbitrage
	FIXNoise                            // FIX 4.4 noise orders (domain knowledge signal)
)

func (a Archetype) String() string {
	return [...]string{"NoiseTrader", "MarketMaker", "Momentum", "InstitutionalSlicer", "LatencyArb", "FIXNoise"}[a]
}

// Side is buy or sell.
type Side int

const (
	Buy  Side = iota
	Sell
)

// OrderType is the order classification.
type OrderType int

const (
	Limit  OrderType = iota
	Market
	Cancel
)

// Order represents an order to be sent to the contestant's matching engine.
type Order struct {
	ID        string
	Symbol    string
	Side      Side
	Type      OrderType
	Price     float64
	Quantity  int64
	Archetype Archetype
	EmittedAt time.Time
}

// Fill is the response from the matching engine for an order.
type Fill struct {
	OrderID   string
	FillPrice float64
	FillQty   int64
	Timestamp time.Time
	Latency   time.Duration // app-layer RTT from bot perspective
}

// Metrics is collected per bot and sent to telemetry.
type Metrics struct {
	OrderID   string
	Archetype Archetype
	AppRTTNS  int64
	Correct   bool
	FillPrice float64
	FillQty   int64
	EmittedNS int64
}

// Bot is the interface all archetype workers implement.
type Bot interface {
	Run(ctx context.Context, endpoint string, out chan<- Metrics) error
	Archetype() Archetype
}

// Config holds per-bot configuration.
type Config struct {
	Symbol      string
	TargetTPS   int
	SessionID   string
	SandboxID   string
	HTTPClient  *http.Client // shared across the fleet; never nil
	FIXEndpoint string       // TCP addr for FIX 4.4 acceptor (empty = REST only)
}
