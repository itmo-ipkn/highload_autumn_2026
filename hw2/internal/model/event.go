package model

import (
	"encoding/json"
	"time"
)

const (
	TopicEvents = "events"
	TopicDLT    = "events.dlt"

	KeyModeUserId = "user_id"
	KeyModeNone   = "none"
)

var (
	EventTypes = []string{"page_view", "add_to_cart", "purchase"}
)

type Event struct {
	EventID    string          `json:"event_id"`
	UserID     string          `json:"user_id"`
	Type       string          `json:"type"`
	Seq        int64           `json:"seq"`
	ProducedAt time.Time       `json:"produced_at"`
	Payload    json.RawMessage `json:"payload"`
}

type Payload struct {
	Poison     bool   `json:"poison"`
	ProducerID string `json:"producer_id"`
	Message    string `json:"message"`
}

type SinkEvent struct {
	EventID    string
	UserID     string
	Seq        int64
	WriteOrder int64
}

func IsPoison(event Event) bool {
	var payload Payload
	return json.Unmarshal(event.Payload, &payload) != nil || payload.Poison
}
