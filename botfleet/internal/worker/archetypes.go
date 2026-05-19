package worker

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// ── Shared HTTP client ─────────────────────────────────────────────────────────

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 200,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

// ── 1. Noise Trader ───────────────────────────────────────────────────────────
// Sends random Limit/Market orders. Baseline bot; creates background noise.

type NoiseBot struct {
	cfg Config
	rng *rand.Rand
}

func NewNoiseBot(cfg Config) *NoiseBot {
	return &NoiseBot{cfg: cfg, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

func (b *NoiseBot) Archetype() Archetype { return NoiseTrader }

func (b *NoiseBot) Run(ctx context.Context, endpoint string, out chan<- Metrics) error {
	client := newHTTPClient()
	ticker := time.NewTicker(rateToInterval(b.cfg.TargetTPS))
	defer ticker.Stop()

	midPrice := 100.0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			side := Side(b.rng.Intn(2))
			price := midPrice * (1 + (b.rng.Float64()-0.5)*0.02)
			o := Order{
				ID: newOrderID(), Symbol: b.cfg.Symbol, Side: side,
				Type: Limit, Price: price, Quantity: int64(b.rng.Intn(10) + 1),
				Archetype: NoiseTrader, EmittedAt: time.Now(),
			}
			fill, rtt, err := sendOrder(ctx, client, endpoint, o)
			if err != nil {
				continue
			}
			out <- Metrics{
				OrderID:   o.ID,
				Archetype: NoiseTrader,
				AppRTTNS:  rtt.Nanoseconds(),
				Correct:   fill.FillPrice > 0 || fill.Status == "PENDING",
				FillPrice: fill.FillPrice,
				FillQty:   fill.FillQty,
				EmittedNS: o.EmittedAt.UnixNano(),
			}
		}
	}
}

// ── 2. Market Maker (Avellaneda-Stoikov) ─────────────────────────────────────
// Quotes a bid and ask around the fair price, adjusting spread based on
// inventory risk using the A-S model: δ_bid/ask = γσ²(T-t) ± γσ²(T-t)/2 + ln(1+γ/k)/γ

type MarketMakerBot struct {
	cfg       Config
	rng       *rand.Rand
	inventory int64   // signed inventory in base asset
	gamma     float64 // risk aversion (0.1)
	sigma     float64 // volatility estimate
	kappa     float64 // order book liquidity param
}

func NewMarketMakerBot(cfg Config) *MarketMakerBot {
	return &MarketMakerBot{
		cfg: cfg, rng: rand.New(rand.NewSource(time.Now().UnixNano())),
		gamma: 0.1, sigma: 0.02, kappa: 1.5,
	}
}

func (b *MarketMakerBot) Archetype() Archetype { return MarketMaker }

func (b *MarketMakerBot) Run(ctx context.Context, endpoint string, out chan<- Metrics) error {
	client := newHTTPClient()
	ticker := time.NewTicker(rateToInterval(b.cfg.TargetTPS))
	defer ticker.Stop()

	midPrice := 100.0
	T := 1.0 // horizon in "time units"
	t := 0.0

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			t += 0.001
			if t >= T {
				t = 0
			}
			dt := T - t

			// A-S reservation price: r = s - q*γ*σ²*(T-t)
			q := float64(b.inventory)
			reservation := midPrice - q*b.gamma*b.sigma*b.sigma*dt

			// A-S optimal spread: δ = γ*σ²*(T-t) + (2/γ)*ln(1+γ/κ)
			spread := b.gamma*b.sigma*b.sigma*dt + (2/b.gamma)*math.Log(1+b.gamma/b.kappa)
			halfSpread := spread / 2

			bidPrice := reservation - halfSpread
			askPrice := reservation + halfSpread

			// Submit bid
			bid := Order{
				ID: newOrderID(), Symbol: b.cfg.Symbol, Side: Buy,
				Type: Limit, Price: bidPrice, Quantity: 1,
				Archetype: MarketMaker, EmittedAt: time.Now(),
			}
			if fill, rtt, err := sendOrder(ctx, client, endpoint, bid); err == nil {
				if fill.FillQty > 0 {
					b.inventory += fill.FillQty
				}
				out <- Metrics{
					OrderID: bid.ID, Archetype: MarketMaker, AppRTTNS: rtt.Nanoseconds(),
					Correct: true, FillPrice: fill.FillPrice, FillQty: fill.FillQty,
					EmittedNS: bid.EmittedAt.UnixNano(),
				}
			}

			// Submit ask
			ask := Order{
				ID: newOrderID(), Symbol: b.cfg.Symbol, Side: Sell,
				Type: Limit, Price: askPrice, Quantity: 1,
				Archetype: MarketMaker, EmittedAt: time.Now(),
			}
			if fill, rtt, err := sendOrder(ctx, client, endpoint, ask); err == nil {
				if fill.FillQty > 0 {
					b.inventory -= fill.FillQty
				}
				out <- Metrics{
					OrderID: ask.ID, Archetype: MarketMaker, AppRTTNS: rtt.Nanoseconds(),
					Correct: true, FillPrice: fill.FillPrice, FillQty: fill.FillQty,
					EmittedNS: ask.EmittedAt.UnixNano(),
				}
			}

			// Update mid-price estimate (GBM random walk)
			midPrice *= math.Exp((b.rng.Float64()-0.5)*b.sigma*0.001)
		}
	}
}

// ── 3. Momentum Trader ────────────────────────────────────────────────────────
// Tracks a moving average; buys on uptrend, sells on downtrend.
// Creates cascading order flow that stresses queue management.

type MomentumBot struct {
	cfg    Config
	rng    *rand.Rand
	prices []float64
	window int
}

func NewMomentumBot(cfg Config) *MomentumBot {
	return &MomentumBot{cfg: cfg, rng: rand.New(rand.NewSource(time.Now().UnixNano())), window: 20}
}

func (b *MomentumBot) Archetype() Archetype { return Momentum }

func (b *MomentumBot) Run(ctx context.Context, endpoint string, out chan<- Metrics) error {
	client := newHTTPClient()
	ticker := time.NewTicker(rateToInterval(b.cfg.TargetTPS))
	defer ticker.Stop()

	midPrice := 100.0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// Simulate price observation (in production this reads from the last fill)
			midPrice *= math.Exp((b.rng.Float64()-0.5)*0.01)
			b.prices = append(b.prices, midPrice)
			if len(b.prices) > b.window*2 {
				b.prices = b.prices[1:]
			}
			if len(b.prices) < b.window {
				continue
			}

			shortMA := avg(b.prices[len(b.prices)-5:])
			longMA := avg(b.prices[len(b.prices)-b.window:])

			var side Side
			if shortMA > longMA*1.001 {
				side = Buy
			} else if shortMA < longMA*0.999 {
				side = Sell
			} else {
				continue
			}

			o := Order{
				ID: newOrderID(), Symbol: b.cfg.Symbol, Side: side,
				Type: Market, Quantity: int64(b.rng.Intn(5) + 1),
				Archetype: Momentum, EmittedAt: time.Now(),
			}
			fill, rtt, err := sendOrder(ctx, client, endpoint, o)
			if err != nil {
				continue
			}
			out <- Metrics{
				OrderID: o.ID, Archetype: Momentum, AppRTTNS: rtt.Nanoseconds(),
				Correct: fill.FillQty > 0 || fill.Status == "PENDING",
				FillPrice: fill.FillPrice, FillQty: fill.FillQty,
				EmittedNS: o.EmittedAt.UnixNano(),
			}
		}
	}
}

// ── 4. Institutional Slicer ───────────────────────────────────────────────────
// Splits a large "parent" order into time-randomised iceberg child slices
// (VWAP/TWAP style). Tests the matching engine's handling of sustained flow.

type InstitutionalSlicerBot struct {
	cfg         Config
	rng         *rand.Rand
	parentSize  int64 // total size of the institutional order
	sliceSize   int64 // max size per slice
}

func NewInstitutionalSlicerBot(cfg Config) *InstitutionalSlicerBot {
	return &InstitutionalSlicerBot{
		cfg: cfg, rng: rand.New(rand.NewSource(time.Now().UnixNano())),
		parentSize: 500, sliceSize: 10,
	}
}

func (b *InstitutionalSlicerBot) Archetype() Archetype { return InstitutionalSlicer }

func (b *InstitutionalSlicerBot) Run(ctx context.Context, endpoint string, out chan<- Metrics) error {
	client := newHTTPClient()
	midPrice := 100.0

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// New parent order
		remaining := b.parentSize
		side := Side(b.rng.Intn(2))
		parentPrice := midPrice * (1 + (b.rng.Float64()-0.5)*0.005)

		for remaining > 0 {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			qty := b.sliceSize
			if remaining < qty {
				qty = remaining
			}
			remaining -= qty

			// TWAP: random sleep between slices (50-200ms)
			jitter := time.Duration(50+b.rng.Intn(150)) * time.Millisecond
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(jitter):
			}

			o := Order{
				ID: newOrderID(), Symbol: b.cfg.Symbol, Side: side,
				Type: Limit, Price: parentPrice, Quantity: qty,
				Archetype: InstitutionalSlicer, EmittedAt: time.Now(),
			}
			fill, rtt, err := sendOrder(ctx, client, endpoint, o)
			if err != nil {
				continue
			}
			out <- Metrics{
				OrderID: o.ID, Archetype: InstitutionalSlicer, AppRTTNS: rtt.Nanoseconds(),
				Correct: true, FillPrice: fill.FillPrice, FillQty: fill.FillQty,
				EmittedNS: o.EmittedAt.UnixNano(),
			}
		}

		// Cool-off between parent orders
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// ── 5. Latency Arbitrageur ────────────────────────────────────────────────────
// Fires aggressive Market orders as fast as possible, exploiting any stale
// quotes visible in the book. Maximum concurrency stress on the engine.

type LatencyArbBot struct {
	cfg Config
	rng *rand.Rand
}

func NewLatencyArbBot(cfg Config) *LatencyArbBot {
	return &LatencyArbBot{cfg: cfg, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

func (b *LatencyArbBot) Archetype() Archetype { return LatencyArb }

func (b *LatencyArbBot) Run(ctx context.Context, endpoint string, out chan<- Metrics) error {
	client := newHTTPClient()
	// No rate limiter — fire at max speed to stress-test the engine
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		side := Side(b.rng.Intn(2))
		o := Order{
			ID: newOrderID(), Symbol: b.cfg.Symbol, Side: side,
			Type: Market, Quantity: int64(b.rng.Intn(3) + 1),
			Archetype: LatencyArb, EmittedAt: time.Now(),
		}
		fill, rtt, err := sendOrder(ctx, client, endpoint, o)
		if err != nil {
			time.Sleep(time.Millisecond)
			continue
		}
		out <- Metrics{
			OrderID: o.ID, Archetype: LatencyArb, AppRTTNS: rtt.Nanoseconds(),
			Correct: fill.FillQty > 0 || fill.Status == "PENDING",
			FillPrice: fill.FillPrice, FillQty: fill.FillQty,
			EmittedNS: o.EmittedAt.UnixNano(),
		}
	}
}

// ── Helpers ────────────────────────────────────────────────────────────────────

func newOrderID() string {
	return fmt.Sprintf("ord-%s", uuid.New().String()[:8])
}

func rateToInterval(tps int) time.Duration {
	if tps <= 0 {
		return time.Second
	}
	return time.Duration(float64(time.Second) / float64(tps))
}

func avg(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}
