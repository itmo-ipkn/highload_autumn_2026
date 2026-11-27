// Package publisher публикует события в Kafka через асинхронный продюсер sarama.
package publisher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"highload/hw2/producer/internal/event"
)

// ErrClosed возвращается из Publish после начала остановки.
var ErrClosed = errors.New("publisher is closed")

// Metrics — метрики отправки.
type Metrics interface {
	Enqueued()
	Acked(latency time.Duration)
	Failed(reason string)
	SetInFlight(n int)
}

type noopMetrics struct{}

func (noopMetrics) Enqueued()           {}
func (noopMetrics) Acked(time.Duration) {}
func (noopMetrics) Failed(string)       {}
func (noopMetrics) SetInFlight(int)     {}

// Stats — накопленные счётчики отправки.
type Stats struct {
	Enqueued uint64
	Acked    uint64
	Failed   uint64
	InFlight int
}

// Publisher отправляет события в Kafka. Безопасен для конкурентного использования.
type Publisher struct {
	opts     Options
	producer sarama.AsyncProducer
	log      *slog.Logger
	metrics  Metrics

	// slots — семафор in-flight сообщений (backpressure).
	slots chan struct{}

	// mu защищает closed: Publish берёт RLock, Close — Lock. После AsyncClose
	// писать во входной канал sarama нельзя.
	mu     sync.RWMutex
	closed bool

	closeOnce     sync.Once
	drained       chan struct{} // закрывается, когда дочитаны Successes и Errors
	stopStats     chan struct{}
	stopStatsOnce sync.Once

	enqueued atomic.Uint64
	acked    atomic.Uint64
	failed   atomic.Uint64
}

// msgMeta едет вместе с сообщением в ProducerMessage.Metadata и возвращается в Successes/Errors.
type msgMeta struct {
	eventID    uuid.UUID
	userID     string
	seq        uint64
	enqueuedAt time.Time
}

// Builder собирает Publisher.
type Builder struct {
	opts     Options
	log      *slog.Logger
	metrics  Metrics
	producer sarama.AsyncProducer
}

// NewBuilder начинает сборку Publisher с заданными опциями.
func NewBuilder(opts Options) *Builder {
	return &Builder{opts: opts, log: slog.Default(), metrics: noopMetrics{}}
}

func (b *Builder) WithLogger(l *slog.Logger) *Builder { b.log = l; return b }

func (b *Builder) WithMetrics(m Metrics) *Builder { b.metrics = m; return b }

// WithProducer подставляет готовый AsyncProducer (в тестах — sarama/mocks).
func (b *Builder) WithProducer(p sarama.AsyncProducer) *Builder { b.producer = p; return b }

// Build проверяет опции, при необходимости подключается к Kafka и запускает
// горутины чтения подтверждений.
func (b *Builder) Build() (*Publisher, error) {
	if b.opts.Topic == "" {
		return nil, errors.New("publisher: topic is required")
	}
	if b.opts.MaxInFlight <= 0 {
		return nil, errors.New("publisher: MaxInFlight must be positive")
	}
	producer := b.producer
	if producer == nil {
		cfg := SaramaConfig(b.opts)
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("publisher: sarama config: %w", err)
		}
		var err error
		if producer, err = sarama.NewAsyncProducer(b.opts.Brokers, cfg); err != nil {
			return nil, fmt.Errorf("publisher: connect to %v: %w", b.opts.Brokers, err)
		}
	}

	p := &Publisher{
		opts:      b.opts,
		producer:  producer,
		log:       b.log,
		metrics:   b.metrics,
		slots:     make(chan struct{}, b.opts.MaxInFlight),
		drained:   make(chan struct{}),
		stopStats: make(chan struct{}),
	}
	p.start()
	return p, nil
}

func (p *Publisher) start() {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); p.readSuccesses() }()
	go func() { defer wg.Done(); p.readErrors() }()
	go func() {
		wg.Wait()
		close(p.drained)
	}()
	if p.opts.ReportEvery > 0 {
		go p.reportStats(p.opts.ReportEvery)
	}
}

// Publish кладёт событие в буфер продюсера. Если in-flight сообщений уже MaxInFlight,
// ждёт освобождения слота или отмены ctx. nil означает, что событие принято sarama.
// Итог отправки (ack или ошибка) обрабатывается асинхронно.
func (p *Publisher) Publish(ctx context.Context, e event.Event) error {
	value, err := e.Marshal()
	if err != nil {
		return fmt.Errorf("marshal event %s: %w", e.EventID, err)
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrClosed
	}

	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	msg := &sarama.ProducerMessage{
		Topic: p.opts.Topic,
		Value: sarama.ByteEncoder(value),
		Headers: []sarama.RecordHeader{
			{Key: []byte("x-producer-pod"), Value: []byte(p.opts.PodID)},
			{Key: []byte("content-type"), Value: []byte("application/json")},
		},
		Metadata: &msgMeta{eventID: e.EventID, userID: e.UserID, seq: e.Seq, enqueuedAt: time.Now()},
	}
	if p.opts.KeyByUser {
		msg.Key = sarama.StringEncoder(e.UserID)
	}

	select {
	case p.producer.Input() <- msg:
	case <-ctx.Done():
		<-p.slots
		return ctx.Err()
	}
	p.enqueued.Add(1)
	p.metrics.Enqueued()
	p.metrics.SetInFlight(len(p.slots))
	return nil
}

func (p *Publisher) readSuccesses() {
	for msg := range p.producer.Successes() {
		<-p.slots
		p.acked.Add(1)
		if m, ok := msg.Metadata.(*msgMeta); ok {
			p.metrics.Acked(time.Since(m.enqueuedAt))
		}
		p.metrics.SetInFlight(len(p.slots))
	}
}

// readErrors логирует каждую неудачную отправку: ошибки не теряются молча.
func (p *Publisher) readErrors() {
	for perr := range p.producer.Errors() {
		<-p.slots
		p.failed.Add(1)
		reason := errorReason(perr.Err)
		p.metrics.Failed(reason)
		p.metrics.SetInFlight(len(p.slots))

		attrs := []any{
			slog.String("reason", reason),
			slog.Any("err", perr.Err),
		}
		if perr.Msg != nil {
			attrs = append(attrs, slog.String("topic", perr.Msg.Topic), slog.Int("partition", int(perr.Msg.Partition)))
			if m, ok := perr.Msg.Metadata.(*msgMeta); ok {
				attrs = append(attrs,
					slog.String("user_id", m.userID),
					slog.Uint64("seq", m.seq),
					slog.String("event_id", m.eventID.String()),
				)
			}
		}
		p.log.Error("produce failed: event is lost, seq gap expected", attrs...)
	}
}

func errorReason(err error) string {
	var kerr sarama.KError
	if errors.As(err, &kerr) {
		return kerr.Error()
	}
	switch {
	case errors.Is(err, sarama.ErrOutOfBrokers):
		return "out_of_brokers"
	case errors.Is(err, sarama.ErrShuttingDown):
		return "shutting_down"
	case errors.Is(err, sarama.ErrMessageSizeTooLarge):
		return "message_too_large"
	}
	return "other"
}

func (p *Publisher) reportStats(every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	var prev Stats
	for {
		select {
		case <-t.C:
			s := p.Stats()
			p.log.Info("producer stats",
				slog.Uint64("enqueued", s.Enqueued),
				slog.Uint64("acked", s.Acked),
				slog.Uint64("failed", s.Failed),
				slog.Int("in_flight", s.InFlight),
				slog.Float64("acked_per_sec", float64(s.Acked-prev.Acked)/every.Seconds()),
			)
			prev = s
		case <-p.stopStats:
			return
		}
	}
}

// Stats возвращает текущие счётчики.
func (p *Publisher) Stats() Stats {
	return Stats{
		Enqueued: p.enqueued.Load(),
		Acked:    p.acked.Load(),
		Failed:   p.failed.Load(),
		InFlight: len(p.slots),
	}
}

// Close перестаёт принимать события и досылает всё, что лежит в буфере sarama.
// Возвращает управление, когда на каждое сообщение пришёл ack или ошибка, либо
// когда истёк ctx. В последнем случае неподтверждённые сообщения считаются потерянными.
func (p *Publisher) Close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		p.producer.AsyncClose()
	})

	select {
	case <-p.drained:
		p.stopReporter()
		return nil
	case <-ctx.Done():
		p.stopReporter()
		return fmt.Errorf("flush timed out: %d messages not acknowledged: %w", len(p.slots), ctx.Err())
	}
}

func (p *Publisher) stopReporter() {
	p.stopStatsOnce.Do(func() { close(p.stopStats) })
}
