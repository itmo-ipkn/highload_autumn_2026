package lab2

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type EventType string

const (
	PageView  EventType = "page_view"
	AddToCart EventType = "add_to_cart"
	Purchase  EventType = "purchase"
)

type Event struct {
	EventID    uuid.UUID       `json:"event_id"`
	UserID     uuid.UUID       `json:"user_id"`
	Type       EventType       `json:"type"`
	Seq        uint64          `json:"seq"`
	ProducedAt time.Time       `json:"produced_at"`
	Payload    json.RawMessage `json:"payload"`
	Poison     bool            `json:"poison"`
}
