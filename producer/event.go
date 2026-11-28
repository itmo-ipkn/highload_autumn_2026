package main

import (
	"encoding/json"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	EventID    string          `json:"event_id"`
	UserID     string          `json:"user_id"`
	Type       string          `json:"type"`
	Seq        int             `json:"seq"`
	ProducedAt time.Time       `json:"produced_at"`
	Payload    json.RawMessage `json:"payload"`
}

func generateEvent(userID string, seq int) (Event, error) {
	eventType := randomEventType()
	poison := rand.Float64() < 0.02

	payload, err := generatePayload(eventType, poison)
	if err != nil {
		return Event{}, err
	}

	return Event{
		EventID:    uuid.NewString(),
		UserID:     userID,
		Type:       eventType,
		Seq:        seq,
		ProducedAt: time.Now().UTC(),
		Payload:    payload,
	}, nil
}

func randomEventType() string {
	types := []string{"page_view", "add_to_cart", "purchase"}
	return types[rand.Intn(len(types))]
}

func generatePayload(eventType string, poison bool) (json.RawMessage, error) {
	payload := map[string]interface{}{
		"poison": poison,
	}

	switch eventType {
	case "page_view":
		payload["page"] = "/products/" + randomProductID()
	case "add_to_cart":
		payload["product_id"] = randomProductID()
		payload["quantity"] = rand.Intn(3) + 1
	case "purchase":
		payload["product_id"] = randomProductID()
		payload["amount"] = rand.Intn(10000) + 100
	}

	return json.Marshal(payload)
}

func randomProductID() string {
	return "product_" + uuid.NewString()[:8]
}
