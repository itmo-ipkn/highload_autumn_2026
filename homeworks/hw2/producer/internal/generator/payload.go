package generator

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"

	"github.com/google/uuid"

	"highload/hw2/producer/internal/event"
)

// typeWeights — доли типов событий, похожие на воронку интернет-магазина.
var typeWeights = []struct {
	t      event.Type
	weight int
}{
	{event.TypePageView, 70},
	{event.TypeAddToCart, 20},
	{event.TypePurchase, 10},
}

var referrers = []string{"direct", "search", "email", "ads", "social"}

func pickType(r *rand.Rand) event.Type {
	total := 0
	for _, tw := range typeWeights {
		total += tw.weight
	}
	n := r.IntN(total)
	for _, tw := range typeWeights {
		if n < tw.weight {
			return tw.t
		}
		n -= tw.weight
	}
	return event.TypePageView
}

// buildPayload собирает произвольный JSON, содержимое которого зависит от типа события.
// У ядовитого события дополнительно стоит "poison": true.
func buildPayload(r *rand.Rand, t event.Type, poison bool) (json.RawMessage, error) {
	p := map[string]any{}
	switch t {
	case event.TypePageView:
		p["url"] = fmt.Sprintf("/catalog/%d", r.IntN(1000))
		p["referrer"] = referrers[r.IntN(len(referrers))]
		p["duration_ms"] = 100 + r.IntN(10_000)
	case event.TypeAddToCart:
		p["sku"] = fmt.Sprintf("SKU-%05d", r.IntN(100_000))
		p["qty"] = 1 + r.IntN(5)
		p["price"] = float64(100+r.IntN(100_000)) / 100
	case event.TypePurchase:
		p["order_id"] = uuid.NewString()
		p["items"] = 1 + r.IntN(10)
		p["amount"] = float64(1_000+r.IntN(1_000_000)) / 100
		p["currency"] = "RUB"
	default:
		return nil, fmt.Errorf("unknown event type %q", t)
	}
	if poison {
		p[event.PoisonField] = true
		p["poison_reason"] = "synthetic failure injected by producer"
	}
	return json.Marshal(p)
}
