// Package event описывает модель события — JSON-контракт между продюсером и консьюмером.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Type — тип пользовательского события.
type Type string

const (
	TypePageView  Type = "page_view"
	TypeAddToCart Type = "add_to_cart"
	TypePurchase  Type = "purchase"
)

// Types — все допустимые типы событий.
var Types = []Type{TypePageView, TypeAddToCart, TypePurchase}

// Valid сообщает, входит ли тип в список допустимых.
func (t Type) Valid() bool {
	switch t {
	case TypePageView, TypeAddToCart, TypePurchase:
		return true
	}
	return false
}

// PoisonField — поле payload, по которому консьюмер распознаёт «ядовитое» событие
// и всегда падает при его обработке.
const PoisonField = "poison"

// Event — событие, которое продюсер публикует в Kafka.
type Event struct {
	// EventID — UUID события, ключ идемпотентной записи в sink.
	EventID uuid.UUID `json:"event_id"`
	// UserID — пользователь. Каждый user_id генерирует ровно один под-продюсер.
	UserID string `json:"user_id"`
	Type   Type   `json:"type"`
	// Seq — порядковый номер события пользователя: с 1, без пропусков.
	Seq        uint64          `json:"seq"`
	ProducedAt time.Time       `json:"produced_at"`
	Payload    json.RawMessage `json:"payload"`
}

// Validate проверяет инварианты модели.
func (e Event) Validate() error {
	var errs []error
	if e.EventID == uuid.Nil {
		errs = append(errs, errors.New("event_id is empty"))
	}
	if e.UserID == "" {
		errs = append(errs, errors.New("user_id is empty"))
	}
	if !e.Type.Valid() {
		errs = append(errs, fmt.Errorf("unknown type %q", e.Type))
	}
	if e.Seq < 1 {
		errs = append(errs, errors.New("seq must start from 1"))
	}
	if e.ProducedAt.IsZero() {
		errs = append(errs, errors.New("produced_at is empty"))
	}
	if !json.Valid(e.Payload) {
		errs = append(errs, errors.New("payload is not valid JSON"))
	}
	return errors.Join(errs...)
}

// Marshal сериализует событие в JSON.
func (e Event) Marshal() ([]byte, error) {
	return json.Marshal(e)
}
