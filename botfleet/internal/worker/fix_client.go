package worker

// FIX 4.4 client — implements a minimal New Order Single (MsgType=D) sender
// over a persistent TCP connection.  Bots that use FIX are judged as showing
// trading-domain knowledge; the FIXNoiseBot archetype exercises this path.
//
// Wire format (SOH = 0x01):
//   8=FIX.4.4 SOH 9=<len> SOH 35=D SOH ... 10=<checksum> SOH

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

const soh = "\x01"

// FIX tag constants used by this client.
const (
	tagBeginString  = 8
	tagBodyLength   = 9
	tagMsgType      = 35
	tagSenderCompID = 49
	tagTargetCompID = 56
	tagMsgSeqNum    = 34
	tagSendingTime  = 52
	tagChecksum     = 10
	tagHeartBtInt   = 108

	// NOS-specific
	tagClOrdID     = 11
	tagSymbol       = 55
	tagSide         = 54
	tagTransactTime = 60
	tagOrderQty     = 38
	tagOrdType      = 40
	tagPrice        = 44
	tagText         = 58
)

// FIXClient holds a persistent TCP connection to a FIX acceptor and tracks
// the outbound sequence number.
type FIXClient struct {
	conn     net.Conn
	reader   *bufio.Reader
	seqNum   atomic.Int64
	senderID string
	targetID string
}

// DialFIX connects to a FIX 4.4 acceptor and performs a Logon (35=A).
// Returns an error if the connection or logon fails.
func DialFIX(ctx context.Context, addr, senderID, targetID string) (*FIXClient, error) {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("fix dial %s: %w", addr, err)
	}
	c := &FIXClient{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		senderID: senderID,
		targetID: targetID,
	}
	c.seqNum.Store(1)
	if err := c.sendLogon(); err != nil {
		conn.Close()
		return nil, err
	}
	return c, nil
}

// Close sends a Logout (35=5) and closes the TCP connection.
func (c *FIXClient) Close() {
	_ = c.sendMsg("5", "") // MsgType Logout
	_ = c.conn.Close()
}

// SendNOS sends a New Order Single (35=D) and returns the app-layer RTT.
// The RTT is measured as the time between send and the first matching
// Execution Report (35=8) containing the same ClOrdID.
func (c *FIXClient) SendNOS(ctx context.Context, o Order) (time.Duration, error) {
	side := "1" // Buy
	if o.Side == Sell {
		side = "2"
	}
	ordType := "2" // Limit
	if o.Type == Market {
		ordType = "1"
	}

	now := fixTimestamp(time.Now())
	body := field(tagClOrdID, o.ID) +
		field(tagSymbol, o.Symbol) +
		field(tagSide, side) +
		field(tagTransactTime, now) +
		field(tagOrderQty, fmt.Sprintf("%d", o.Quantity)) +
		field(tagOrdType, ordType)
	if o.Type == Limit {
		body += field(tagPrice, fmt.Sprintf("%.4f", o.Price))
	}

	start := time.Now()
	if err := c.sendMsg("D", body); err != nil {
		return 0, err
	}

	// Read until we get a matching ExecReport or context cancels.
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		raw, err := c.readMsg()
		if err != nil {
			return time.Since(start), err
		}
		tags := parseTags(raw)
		if tags[tagMsgType] == "8" && tags[tagClOrdID] == o.ID {
			return time.Since(start), nil
		}
		// Keep draining — heartbeats, other exec reports, etc.
	}
}

// ── internal helpers ──────────────────────────────────────────────────────────

func (c *FIXClient) sendLogon() error {
	body := field(tagHeartBtInt, "30") // 30s heartbeat
	return c.sendMsg("A", body)
}

func (c *FIXClient) sendMsg(msgType, body string) error {
	seq := c.seqNum.Add(1)
	header := field(tagMsgType, msgType) +
		field(tagSenderCompID, c.senderID) +
		field(tagTargetCompID, c.targetID) +
		field(tagMsgSeqNum, fmt.Sprintf("%d", seq)) +
		field(tagSendingTime, fixTimestamp(time.Now()))

	bodyFull := header + body
	// BodyLength = number of bytes after BodyLength field up to (not incl.) checksum
	fullMsg := fmt.Sprintf("8=FIX.4.4%s9=%d%s%s", soh, len(bodyFull), soh, bodyFull)
	fullMsg += field(tagChecksum, fmt.Sprintf("%03d", checksum(fullMsg)))

	_, err := fmt.Fprint(c.conn, fullMsg)
	return err
}

func (c *FIXClient) readMsg() (string, error) {
	// FIX messages end with 10=xxx\x01 — read until we see that pattern.
	var sb strings.Builder
	for {
		line, err := c.reader.ReadString('\x01')
		if err != nil {
			return "", err
		}
		sb.WriteString(line)
		if strings.Contains(line, "10=") {
			break
		}
	}
	return sb.String(), nil
}

func field(tag int, val string) string {
	return fmt.Sprintf("%d=%s%s", tag, val, soh)
}

func fixTimestamp(t time.Time) string {
	return t.UTC().Format("20060102-15:04:05.000")
}

func checksum(msg string) int {
	sum := 0
	for _, b := range []byte(msg) {
		sum += int(b)
	}
	return sum % 256
}

func parseTags(msg string) map[int]string {
	out := make(map[int]string)
	for _, part := range strings.Split(msg, soh) {
		idx := strings.IndexByte(part, '=')
		if idx < 0 {
			continue
		}
		var tag int
		if _, err := fmt.Sscanf(part[:idx], "%d", &tag); err == nil {
			out[tag] = part[idx+1:]
		}
	}
	return out
}

// ── FIX Noise Bot ──────────────────────────────────────────────────────────────
// Sends random Limit/Market orders via FIX 4.4 TCP. Falls back to REST if
// FIXEndpoint is empty or the connection fails.

type FIXNoiseBot struct {
	cfg Config
	rng *rand.Rand
}

func NewFIXNoiseBot(cfg Config) *FIXNoiseBot {
	return &FIXNoiseBot{cfg: cfg, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

func (b *FIXNoiseBot) Archetype() Archetype { return FIXNoise }

func (b *FIXNoiseBot) Run(ctx context.Context, endpoint string, out chan<- Metrics) error {
	// Attempt FIX connection; fall back to REST if unavailable.
	if b.cfg.FIXEndpoint == "" {
		return b.runREST(ctx, endpoint, out)
	}
	senderID := fmt.Sprintf("BOT%04d", b.rng.Intn(9999))
	fix, err := DialFIX(ctx, b.cfg.FIXEndpoint, senderID, "EXCHANGE")
	if err != nil {
		// FIX acceptor not available — fall back to REST transparently.
		return b.runREST(ctx, endpoint, out)
	}
	defer fix.Close()
	return b.runFIX(ctx, fix, out)
}

func (b *FIXNoiseBot) runFIX(ctx context.Context, fix *FIXClient, out chan<- Metrics) error {
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
				Archetype: FIXNoise, EmittedAt: time.Now(),
			}
			rtt, err := fix.SendNOS(ctx, o)
			if err != nil {
				continue
			}
			out <- Metrics{
				OrderID:   o.ID,
				Archetype: FIXNoise,
				AppRTTNS:  rtt.Nanoseconds(),
				Correct:   true,
				EmittedNS: o.EmittedAt.UnixNano(),
			}
		}
	}
}

func (b *FIXNoiseBot) runREST(ctx context.Context, endpoint string, out chan<- Metrics) error {
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
				Archetype: FIXNoise, EmittedAt: time.Now(),
			}
			fill, rtt, err := sendOrder(ctx, b.cfg.HTTPClient, endpoint, o)
			if err != nil {
				continue
			}
			out <- Metrics{
				OrderID:   o.ID,
				Archetype: FIXNoise,
				AppRTTNS:  rtt.Nanoseconds(),
				Correct:   fill.FillPrice > 0 || fill.Status == "PENDING",
				FillPrice: fill.FillPrice,
				FillQty:   fill.FillQty,
				EmittedNS: o.EmittedAt.UnixNano(),
			}
		}
	}
}
