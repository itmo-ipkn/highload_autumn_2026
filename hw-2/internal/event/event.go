// Package event - модель события и генератор событий продюсера.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
)

// Type - тип события.
type Type string

// Типы событий.
const (
	PageView  Type = "page_view"
	AddToCart Type = "add_to_cart"
	Purchase  Type = "purchase"
)

// Types - все типы событий.
var Types = []Type{PageView, AddToCart, Purchase}

// Event - событие в топике events, value записи - его JSON.
type Event struct {
	EventID    string    `json:"event_id"`
	UserID     string    `json:"user_id"`
	Type       Type      `json:"type"`
	Seq        int64     `json:"seq"`
	ProducedAt time.Time `json:"produced_at"`
	Payload    Payload   `json:"payload"`
}

// Payload - произвольная часть события.
type Payload struct {
	Producer string `json:"producer"`
	// Poison - консьюмер на таком событии всегда падает.
	Poison bool  `json:"poison,omitempty"`
	Value  int64 `json:"value"`
}

// ErrPoison - ошибка обработки poison-события.
var ErrPoison = errors.New("poison event")

// Decode разбирает value записи. Для poison-события возвращает его вместе с ErrPoison.
func Decode(b []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		return Event{}, fmt.Errorf("decode: %w", err)
	}
	if e.EventID == "" || e.UserID == "" || e.Seq < 1 {
		return Event{}, fmt.Errorf("decode: event_id, user_id and seq are required")
	}
	if e.Payload.Poison {
		return e, ErrPoison
	}
	return e, nil
}

// UserIDs - пул пользователей продюсера: u-<producer>-0001 ... u-<producer>-<n>.
// У разных продюсеров пулы не пересекаются, каждый user_id генерирует ровно один продюсер.
func UserIDs(producer string, n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("u-%s-%04d", producer, i+1)
	}
	return ids
}

// Generator выдаёт события пользователей одного продюсера. Не потокобезопасен.
type Generator struct {
	producer   string
	users      []string
	seq        []int64
	poisonRate float64
	rnd        *rand.Rand
	now        func() time.Time
	newID      func() string
}

// NewGenerator создаёт генератор. rnd задаёт выбор пользователя, типа и poison.
func NewGenerator(producer string, users int, poisonRate float64, rnd *rand.Rand) *Generator {
	ids := UserIDs(producer, users)
	return &Generator{
		producer:   producer,
		users:      ids,
		seq:        make([]int64, len(ids)),
		poisonRate: poisonRate,
		rnd:        rnd,
		now:        time.Now,
		newID:      func() string { return uuid.NewString() },
	}
}

// Next выдаёт следующее событие случайного пользователя. seq пользователя растёт на 1
// при каждом вызове: Next зовётся только перед отправкой события.
func (g *Generator) Next() Event {
	i := g.rnd.IntN(len(g.users))
	g.seq[i]++
	return Event{
		EventID:    g.newID(),
		UserID:     g.users[i],
		Type:       Types[g.rnd.IntN(len(Types))],
		Seq:        g.seq[i],
		ProducedAt: g.now().UTC(),
		Payload: Payload{
			Producer: g.producer,
			Poison:   g.rnd.Float64() < g.poisonRate,
			Value:    g.rnd.Int64N(1000),
		},
	}
}

// Seq - последний выданный seq пользователя, 0 если событий не было.
func (g *Generator) Seq(user string) int64 {
	for i, u := range g.users {
		if u == user {
			return g.seq[i]
		}
	}
	return 0
}
