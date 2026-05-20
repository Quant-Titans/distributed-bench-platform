package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// OrderRequest is the JSON body sent to the contestant's REST endpoint.
type OrderRequest struct {
	OrderID  string  `json:"order_id"`
	Symbol   string  `json:"symbol"`
	Side     string  `json:"side"`
	Type     string  `json:"type"`
	Price    float64 `json:"price,omitempty"`
	Quantity int64   `json:"quantity"`
}

// OrderResponse is the response from the contestant's matching engine.
type OrderResponse struct {
	OrderID   string  `json:"order_id"`
	FillPrice float64 `json:"fill_price"`
	FillQty   int64   `json:"fill_qty"`
	Status    string  `json:"status"`
}

// sendOrder sends an order to the contestant's endpoint and returns the fill
// plus the app-layer RTT. All latency measurement happens here; kernel-level
// RTT is measured separately by the eBPF TC hook.
func sendOrder(ctx context.Context, client *http.Client, endpoint string, o Order) (OrderResponse, time.Duration, error) {
	req := OrderRequest{
		OrderID:  o.ID,
		Symbol:   o.Symbol,
		Side:     sideStr(o.Side),
		Type:     typeStr(o.Type),
		Price:    o.Price,
		Quantity: o.Quantity,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return OrderResponse{}, 0, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		endpoint+"/v1/order", bytes.NewReader(body))
	if err != nil {
		return OrderResponse{}, 0, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := client.Do(httpReq)
	rtt := time.Since(start)
	if err != nil {
		return OrderResponse{}, rtt, fmt.Errorf("send order: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return OrderResponse{}, rtt, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var fill OrderResponse
	if err := json.NewDecoder(resp.Body).Decode(&fill); err != nil {
		return OrderResponse{}, rtt, fmt.Errorf("decode fill: %w", err)
	}
	return fill, rtt, nil
}

func sideStr(s Side) string {
	if s == Buy {
		return "BUY"
	}
	return "SELL"
}

func typeStr(t OrderType) string {
	switch t {
	case Limit:
		return "LIMIT"
	case Market:
		return "MARKET"
	case Cancel:
		return "CANCEL"
	}
	return "LIMIT"
}
