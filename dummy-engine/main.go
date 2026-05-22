// dummy-engine is a minimal order matching engine used for smoke testing and demos.
//
// REST API (port 8080 by default, set via LISTEN_ADDR):
//   POST /v1/order    — submit a limit or market order
//   GET  /healthz     — liveness check
//   GET  /stats       — live order book depth per symbol
//
// FIX 4.4 acceptor (port 8443 by default, set via FIX_ADDR):
//   Accepts New Order Single (35=D), responds with Execution Report (35=8).
//   Supports: Logon (A), Heartbeat (0), Logout (5).
//
// NOT submitted as a contestant image — used only for local development and demos.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ── Order types ───────────────────────────────────────────────────────────────

type Side string

const (
	Buy  Side = "BUY"
	Sell Side = "SELL"
)

type OrderRequest struct {
	OrderID  string  `json:"order_id"`
	Symbol   string  `json:"symbol"`
	Side     Side    `json:"side"`
	Type     string  `json:"type"`
	Price    float64 `json:"price"`
	Quantity int64   `json:"quantity"`
}

type OrderResponse struct {
	OrderID   string  `json:"order_id"`
	FillPrice float64 `json:"fill_price"`
	FillQty   int64   `json:"fill_qty"`
	Status    string  `json:"status"`
}

type order struct {
	id        string
	price     float64
	qty       int64
	side      Side
	arrivedAt time.Time
}

// ── Order book ────────────────────────────────────────────────────────────────

type OrderBook struct {
	mu   sync.Mutex
	bids []*order // sorted: highest price first, then earliest first
	asks []*order // sorted: lowest price first, then earliest first
}

func (ob *OrderBook) match(o *order) (fillPrice float64, fillQty int64) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if o.side == Buy {
		for len(ob.asks) > 0 && o.qty > 0 {
			best := ob.asks[0]
			if best.price > o.price && o.price > 0 {
				break
			}
			fill := min64(o.qty, best.qty)
			fillPrice = best.price
			fillQty += fill
			o.qty -= fill
			best.qty -= fill
			if best.qty == 0 {
				ob.asks = ob.asks[1:]
			}
		}
		if o.qty > 0 {
			ob.bids = append(ob.bids, o)
			sortBids(ob.bids)
		}
	} else {
		for len(ob.bids) > 0 && o.qty > 0 {
			best := ob.bids[0]
			if best.price < o.price && o.price > 0 {
				break
			}
			fill := min64(o.qty, best.qty)
			fillPrice = best.price
			fillQty += fill
			o.qty -= fill
			best.qty -= fill
			if best.qty == 0 {
				ob.bids = ob.bids[1:]
			}
		}
		if o.qty > 0 {
			ob.asks = append(ob.asks, o)
			sortAsks(ob.asks)
		}
	}
	return
}

var books sync.Map // symbol → *OrderBook

func getBook(symbol string) *OrderBook {
	v, _ := books.LoadOrStore(symbol, &OrderBook{})
	return v.(*OrderBook)
}

// ── REST handlers ─────────────────────────────────────────────────────────────

func handleOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	o := &order{
		id:        req.OrderID,
		price:     req.Price,
		qty:       req.Quantity,
		side:      req.Side,
		arrivedAt: time.Now(),
	}

	book := getBook(req.Symbol)
	time.Sleep(time.Duration(50+rand.Intn(450)) * time.Microsecond) // simulate processing
	fillPrice, fillQty := book.match(o)

	resp := OrderResponse{
		OrderID:   req.OrderID,
		FillPrice: fillPrice,
		FillQty:   fillQty,
		Status:    "PENDING",
	}
	if fillQty > 0 {
		resp.Status = "FILLED"
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"status":"ok","engine":"dummy-matching-engine","fix":"enabled"}`)
}

func handleStats(w http.ResponseWriter, _ *http.Request) {
	stats := map[string]any{}
	books.Range(func(k, v any) bool {
		ob := v.(*OrderBook)
		ob.mu.Lock()
		stats[k.(string)] = map[string]int{"bids": len(ob.bids), "asks": len(ob.asks)}
		ob.mu.Unlock()
		return true
	})
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

// ── FIX 4.4 acceptor ─────────────────────────────────────────────────────────
//
// Implements a minimal FIX 4.4 session layer:
//   Logon (35=A)         → respond with Logon
//   Heartbeat (35=0)     → respond with Heartbeat
//   Logout (35=5)        → respond with Logout, close connection
//   New Order Single (D) → respond with Execution Report (35=8)

const soh = "\x01"

func startFIXAcceptor(ctx context.Context, addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("FIX acceptor: listen %s: %v (FIX disabled)", addr, err)
		return
	}
	log.Printf("FIX 4.4 acceptor listening on %s", addr)

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("FIX accept: %v", err)
			continue
		}
		go handleFIXConn(conn)
	}
}

func handleFIXConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	var seqNum int64 = 1

	for {
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		raw, err := readFIXMsg(reader)
		if err != nil {
			return
		}
		tags := parseFIXTags(raw)
		msgType := tags[35]
		peerSender := tags[49]
		peerTarget := tags[56]
		// Our role: we are the exchange (target → sender in responses)
		mySender := peerTarget
		myTarget := peerSender

		switch msgType {
		case "A": // Logon
			sendFIXMsg(conn, &seqNum, "A", mySender, myTarget, fixField(108, "30"))
		case "0": // Heartbeat
			testReqID := tags[112]
			body := ""
			if testReqID != "" {
				body = fixField(112, testReqID)
			}
			sendFIXMsg(conn, &seqNum, "0", mySender, myTarget, body)
		case "5": // Logout
			sendFIXMsg(conn, &seqNum, "5", mySender, myTarget, "")
			return
		case "D": // New Order Single
			handleFIXNOS(conn, &seqNum, tags, mySender, myTarget)
		}
	}
}

func handleFIXNOS(conn net.Conn, seqNum *int64, tags map[int]string, senderID, targetID string) {
	clOrdID := tags[11]
	symbol := tags[55]
	side := tags[54]
	ordType := tags[40]
	qty := tags[38]
	price := tags[44]

	if price == "" {
		price = "100.0000"
	}

	// Simulate matching: market orders always fill, limit orders ~70% fill.
	execType := "F" // Trade (filled)
	ordStatus := "2" // Filled
	fillQty := qty
	avgPx := price

	if ordType == "2" && rand.Float64() < 0.30 {
		// Limit order rests on the book
		execType = "0"  // New
		ordStatus = "0" // New
		fillQty = "0"
		avgPx = "0"
	}

	leavesQty := "0"
	if ordStatus == "0" {
		leavesQty = qty
	}

	now := time.Now().UTC().Format("20060102-15:04:05.000")
	execID := strconv.FormatInt(time.Now().UnixNano(), 36)
	orderID := "O" + execID

	body := fixField(11, clOrdID) +
		fixField(37, orderID) +
		fixField(17, execID) +
		fixField(150, execType) +
		fixField(39, ordStatus) +
		fixField(55, symbol) +
		fixField(54, side) +
		fixField(38, qty) +
		fixField(14, fillQty) +
		fixField(151, leavesQty) +
		fixField(6, avgPx) +
		fixField(60, now)

	sendFIXMsg(conn, seqNum, "8", senderID, targetID, body)
}

func sendFIXMsg(conn net.Conn, seqNum *int64, msgType, senderID, targetID, body string) {
	*seqNum++
	now := time.Now().UTC().Format("20060102-15:04:05.000")
	header := fixField(35, msgType) +
		fixField(49, senderID) +
		fixField(56, targetID) +
		fixField(34, strconv.FormatInt(*seqNum, 10)) +
		fixField(52, now)

	fullBody := header + body
	msg := fmt.Sprintf("8=FIX.4.4%s9=%d%s%s", soh, len(fullBody), soh, fullBody)
	cs := 0
	for _, b := range []byte(msg) {
		cs += int(b)
	}
	msg += fixField(10, fmt.Sprintf("%03d", cs%256))

	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, _ = fmt.Fprint(conn, msg)
}

func readFIXMsg(r *bufio.Reader) (string, error) {
	var sb strings.Builder
	for {
		chunk, err := r.ReadString('\x01')
		if err != nil {
			return "", err
		}
		sb.WriteString(chunk)
		if strings.Contains(chunk, "10=") {
			break
		}
	}
	return sb.String(), nil
}

func fixField(tag int, val string) string {
	return fmt.Sprintf("%d=%s%s", tag, val, soh)
}

func parseFIXTags(msg string) map[int]string {
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

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	httpAddr := envOr("LISTEN_ADDR", ":9000")
	fixAddr  := envOr("FIX_ADDR", ":8443")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// FIX 4.4 acceptor (non-blocking — runs in background goroutine)
	go startFIXAcceptor(ctx, fixAddr)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/order", handleOrder)
	mux.HandleFunc("/healthz", handleHealth)
	mux.HandleFunc("/stats", handleStats)

	srv := &http.Server{
		Addr:        httpAddr,
		Handler:     mux,
		ReadTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("dummy matching engine REST listening on %s", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http serve: %v", err)
		}
	}()

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func sortBids(bids []*order) {
	sort.Slice(bids, func(i, j int) bool {
		if bids[i].price != bids[j].price {
			return bids[i].price > bids[j].price
		}
		return bids[i].arrivedAt.Before(bids[j].arrivedAt)
	})
}

func sortAsks(asks []*order) {
	sort.Slice(asks, func(i, j int) bool {
		if asks[i].price != asks[j].price {
			return asks[i].price < asks[j].price
		}
		return asks[i].arrivedAt.Before(asks[j].arrivedAt)
	})
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
