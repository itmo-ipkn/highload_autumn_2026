package main

import (
	"encoding/json"
	"time"
)

type Event struct {
	EventID    string          `json:"event_id"`
	UserID     string          `json:"user_id"`
	Type       string          `json:"type"`
	Seq        int             `json:"seq"`
	ProducedAt time.Time       `json:"produced_at"`
	Payload    json.RawMessage `json:"payload"`
}
