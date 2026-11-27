// Package generator генерирует события пользователей этого пода с заданной скоростью.
package generator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"

	"highload/hw2/producer/internal/event"
)

// Publisher отправляет событие в Kafka. Возврат nil означает, что брокер подтвердил запись.
type Publisher interface {
	Publish(ctx context.Context, e event.Event) error
}

// Metrics — счётчики генератора.
type Metrics interface {
	EventGenerated(t event.Type, poison bool)
}

type noopMetrics struct{}

func (noopMetrics) EventGenerated(event.Type, bool) {}

// Generator генерирует события для фиксированного набора пользователей.
type Generator struct {
	pub     Publisher
	seq     *Sequencer
	users   []string
	limiter *rate.Limiter
	workers int

	poisonRate float64
	maxEvents  uint64
	reserved   atomic.Uint64

	seed    uint64
	now     func() time.Time
	newID   func() uuid.UUID
	log     *slog.Logger
	metrics Metrics
}

// Option — функциональная опция генератора.
type Option func(*Generator)

// WithRate задаёт темп (событий в секунду на под) и допустимый всплеск.
func WithRate(perSec float64, burst int) Option {
	return func(g *Generator) { g.limiter = rate.NewLimiter(rate.Limit(perSec), burst) }
}

// WithUnlimitedRate снимает ограничение темпа (для тестов).
func WithUnlimitedRate() Option {
	return func(g *Generator) { g.limiter = rate.NewLimiter(rate.Inf, 1) }
}

// WithWorkers задаёт число параллельно генерирующих горутин.
func WithWorkers(n int) Option { return func(g *Generator) { g.workers = n } }

// WithPoisonRate задаёт долю ядовитых событий, от 0 до 1.
func WithPoisonRate(p float64) Option { return func(g *Generator) { g.poisonRate = p } }

// WithMaxEvents останавливает генерацию после n событий. 0 означает без ограничения.
func WithMaxEvents(n uint64) Option { return func(g *Generator) { g.maxEvents = n } }

// WithSeed делает выбор пользователей, типов и poison воспроизводимым.
func WithSeed(seed uint64) Option { return func(g *Generator) { g.seed = seed } }

// WithClock подменяет источник времени для produced_at.
func WithClock(now func() time.Time) Option { return func(g *Generator) { g.now = now } }

// WithIDGenerator подменяет генератор event_id.
func WithIDGenerator(f func() uuid.UUID) Option { return func(g *Generator) { g.newID = f } }

func WithLogger(l *slog.Logger) Option { return func(g *Generator) { g.log = l } }

func WithMetrics(m Metrics) Option { return func(g *Generator) { g.metrics = m } }

// WithSequencer позволяет передать общий Sequencer (например, чтобы печатать снимок при остановке).
func WithSequencer(s *Sequencer) Option { return func(g *Generator) { g.seq = s } }

// UserIDs возвращает пользователей пода. Формат p{pod}-{run}-u{n} гарантирует, что
// пользователя генерирует ровно один под и ровно один его запуск.
func UserIDs(podID, runID string, n int) []string {
	ids := make([]string, n)
	for i := range n {
		ids[i] = fmt.Sprintf("p%s-%s-u%03d", podID, runID, i+1)
	}
	return ids
}

// New собирает генератор. pub и users обязательны.
func New(pub Publisher, users []string, opts ...Option) (*Generator, error) {
	g := &Generator{
		pub:        pub,
		users:      users,
		limiter:    rate.NewLimiter(100, 10),
		workers:    1,
		poisonRate: 0.02,
		seed:       uint64(time.Now().UnixNano()),
		now:        time.Now,
		newID:      uuid.New,
		log:        slog.Default(),
		metrics:    noopMetrics{},
	}
	for _, opt := range opts {
		opt(g)
	}
	if g.seq == nil {
		g.seq = NewSequencer()
	}
	switch {
	case g.pub == nil:
		return nil, errors.New("generator: publisher is required")
	case len(g.users) == 0:
		return nil, errors.New("generator: at least one user is required")
	case g.workers <= 0:
		return nil, errors.New("generator: workers must be positive")
	case g.poisonRate < 0 || g.poisonRate > 1:
		return nil, errors.New("generator: poison rate must be within [0, 1]")
	}
	return g, nil
}

// Sequencer возвращает счётчики seq генератора.
func (g *Generator) Sequencer() *Sequencer { return g.seq }

// Run генерирует события, пока не отменят ctx или не будет достигнут MaxEvents.
// Отмена прекращает генерацию; ошибка уже принятой отправки всё равно возвращается.
func (g *Generator) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	for w := range g.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.work(ctx, cancel, rand.New(rand.NewPCG(g.seed, uint64(w)))); err != nil {
				errOnce.Do(func() { firstErr = err })
				cancel()
			}
		}()
	}
	wg.Wait()
	return firstErr
}

func (g *Generator) work(ctx context.Context, cancel context.CancelFunc, r *rand.Rand) error {
	for {
		if err := g.limiter.Wait(ctx); err != nil {
			return nil // ctx отменён: штатная остановка
		}
		if g.maxEvents > 0 && g.reserved.Add(1) > g.maxEvents {
			return nil
		}

		user := g.users[r.IntN(len(g.users))]
		err := g.seq.Do(user, func(seq uint64) error {
			if err := ctx.Err(); err != nil {
				return err // Могли ждать предыдущего ACK под мьютексом пользователя.
			}
			e, poison, err := g.build(r, user, seq)
			if err == nil {
				err = g.pub.Publish(ctx, e)
			}
			if err != nil {
				if err == ctx.Err() {
					return err
				}
				cancel() // Остановить остальных до освобождения seq.
				return fmt.Errorf("generate event for %s: %w", user, err)
			}
			g.metrics.EventGenerated(e.Type, poison)
			return nil
		})
		if err != nil {
			if err == ctx.Err() {
				return nil
			}
			return err
		}
	}
}

func (g *Generator) build(r *rand.Rand, user string, seq uint64) (event.Event, bool, error) {
	t := pickType(r)
	poison := r.Float64() < g.poisonRate
	payload, err := buildPayload(r, t, poison)
	if err != nil {
		return event.Event{}, false, err
	}
	return event.Event{
		EventID:    g.newID(),
		UserID:     user,
		Type:       t,
		Seq:        seq,
		ProducedAt: g.now().UTC(),
		Payload:    payload,
	}, poison, nil
}
