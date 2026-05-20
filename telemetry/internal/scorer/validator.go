package scorer

import (
	"sync"
)

// PriceTimePriorityValidator checks that fills respect price-time priority:
// - Higher bids are filled before lower bids.
// - Lower asks are filled before higher asks.
// - At the same price, earlier orders are filled first.
//
// This is the correctness engine. Any violation is logged and counted.

type side int

const (
	bidSide side = iota
	askSide
)

type pendingOrder struct {
	id        string
	price     float64
	side      side
	seq       int64 // arrival sequence number — lower = earlier
	filled    bool
}

type Validator struct {
	mu         sync.Mutex
	bids       []*pendingOrder // sorted desc by price, then asc by seq
	asks       []*pendingOrder // sorted asc by price, then asc by seq
	seq        int64
	violations int64
	total      int64
	correct    int64
}

func NewValidator() *Validator {
	return &Validator{}
}

// RecordOrder notes an incoming order (before fill).
func (v *Validator) RecordOrder(id string, price float64, isBuy bool) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.seq++
	o := &pendingOrder{id: id, price: price, seq: v.seq}
	if isBuy {
		o.side = bidSide
		v.bids = insertBid(v.bids, o)
	} else {
		o.side = askSide
		v.asks = insertAsk(v.asks, o)
	}
}

// RecordFill checks that the filled order was the correct next order to fill.
// Returns true if the fill was valid under price-time priority.
func (v *Validator) RecordFill(id string, fillPrice float64, fillQty int64) bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.total++

	// Find the order in pending lists
	o := v.findOrder(id)
	if o == nil {
		// Fill for an unknown order — phantom fill
		v.violations++
		return false
	}

	// Check: was there a better-priority order that should have been filled first?
	if o.side == bidSide {
		for _, b := range v.bids {
			if b.id == id {
				break
			}
			if !b.filled && (b.price > o.price || (b.price == o.price && b.seq < o.seq)) {
				v.violations++
				o.filled = true
				return false
			}
		}
	} else {
		for _, a := range v.asks {
			if a.id == id {
				break
			}
			if !a.filled && (a.price < o.price || (a.price == o.price && a.seq < o.seq)) {
				v.violations++
				o.filled = true
				return false
			}
		}
	}

	o.filled = true
	v.correct++
	return true
}

// Stats returns (total, correct, violations).
func (v *Validator) Stats() (int64, int64, int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.total, v.correct, v.violations
}

func (v *Validator) findOrder(id string) *pendingOrder {
	for _, o := range v.bids {
		if o.id == id {
			return o
		}
	}
	for _, o := range v.asks {
		if o.id == id {
			return o
		}
	}
	return nil
}

// insertBid keeps bids sorted: highest price first, then lowest seq first.
func insertBid(bids []*pendingOrder, o *pendingOrder) []*pendingOrder {
	bids = append(bids, o)
	for i := len(bids) - 1; i > 0; i-- {
		a, b := bids[i-1], bids[i]
		if b.price > a.price || (b.price == a.price && b.seq < a.seq) {
			bids[i-1], bids[i] = b, a
		} else {
			break
		}
	}
	return bids
}

// insertAsk keeps asks sorted: lowest price first, then lowest seq first.
func insertAsk(asks []*pendingOrder, o *pendingOrder) []*pendingOrder {
	asks = append(asks, o)
	for i := len(asks) - 1; i > 0; i-- {
		a, b := asks[i-1], asks[i]
		if b.price < a.price || (b.price == a.price && b.seq < a.seq) {
			asks[i-1], asks[i] = b, a
		} else {
			break
		}
	}
	return asks
}
