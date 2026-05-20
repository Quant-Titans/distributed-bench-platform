// dummy-engine is a minimal order matching engine used for smoke testing.
//
// It accepts the same REST API the bot fleet sends orders to, performs
// price-time priority matching in memory, and returns realistic fill responses.
// Used in docker-compose for local end-to-end testing — NOT submitted as a
// contestant image.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

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
	id       string
	price    float64
	qty      int64
	side     Side
	arrivedAt time.Time
}

// OrderBook maintains bids and asks with price-time priority.
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
			if best.price > o.price && o.price > 0 { // market orders always match
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
		if o.qty > 0 { // remaining goes on the book
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

	// Add tiny realistic latency (50–500µs) to simulate processing time
	time.Sleep(time.Duration(50+rand.Intn(450)) * time.Microsecond)

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
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"status":"ok","engine":"dummy-matching-engine"}`)
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

func main() {
	addr := envOr("LISTEN_ADDR", ":9000")

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/order", handleOrder)
	mux.HandleFunc("/healthz", handleHealth)
	mux.HandleFunc("/stats", handleStats)

	log.Printf("dummy matching engine listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

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
