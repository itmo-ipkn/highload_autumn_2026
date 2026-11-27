// Package event описывает JSON-контракт события, публикуемого producer.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Type string

const (
	TypePageView  Type = "page_view"
	TypeAddToCart Type = "add_to_cart"
	TypePurchase  Type = "purchase"
)

func (t Type) Valid() bool {
	switch t {
	case TypePageView, TypeAddToCart, TypePurchase:
		return true
	default:
		return false
	}
}

type Event struct {
	EventID    uuid.UUID       `json:"event_id"`
	UserID     string          `json:"user_id"`
	Type       Type            `json:"type"`
	Seq        uint64          `json:"seq"`
	ProducedAt time.Time       `json:"produced_at"`
	Payload    json.RawMessage `json:"payload"`
}

func (e Event) Validate() error {
	var errs []error
	if e.EventID == uuid.Nil {
		errs = append(errs, errors.New("event_id is empty"))
	}
	if e.UserID == "" {
		errs = append(errs, errors.New("user_id is empty"))
	}
	if !e.Type.Valid() {
		errs = append(errs, fmt.Errorf("unknown event type %q", e.Type))
	}
	if e.Seq == 0 {
		errs = append(errs, errors.New("seq must be at least 1"))
	}
	if e.ProducedAt.IsZero() {
		errs = append(errs, errors.New("produced_at is empty"))
	}
	if !json.Valid(e.Payload) {
		errs = append(errs, errors.New("payload is not valid JSON"))
	}
	return errors.Join(errs...)
}
