// Package consumer - обработка записей топика events: повторы, DLT, запись в sink.
package consumer

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/event"
	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/sink"
)

// Sink - куда пишутся обработанные события.
type Sink interface {
	Insert(ctx context.Context, r sink.Row) (duplicate bool, err error)
}

// DLT публикует запись в dead letter topic и ждёт подтверждения.
type DLT interface {
	Publish(ctx context.Context, r *kgo.Record) error
}

// Outcome - чем закончилась обработка записи.
type Outcome string

// Исходы обработки.
const (
	OutcomeOK        Outcome = "ok"
	OutcomeDuplicate Outcome = "duplicate"
	OutcomeDLT       Outcome = "dlt"
)

// Metrics консьюмера.
type Metrics struct {
	Events          *prometheus.CounterVec
	AttemptFailures *prometheus.CounterVec
	SinkErrors      prometheus.Counter
	DLTErrors       prometheus.Counter
	Latency         prometheus.Histogram
	Assigned        *prometheus.GaugeVec
	Rebalances      *prometheus.CounterVec
	Commits         *prometheus.CounterVec
}

// NewMetrics регистрирует метрики в reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		Events: f.NewCounterVec(prometheus.CounterOpts{
			Name: "consumer_events_total",
			Help: "Processed records by result: ok (written), duplicate (dropped by event_id), dlt.",
		}, []string{"partition", "result"}),
		AttemptFailures: f.NewCounterVec(prometheus.CounterOpts{
			Name: "consumer_attempt_failures_total",
			Help: "Failed processing attempts.",
		}, []string{"reason"}),
		SinkErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "consumer_sink_errors_total",
			Help: "Failed sink writes, retried.",
		}),
		DLTErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "consumer_dlt_errors_total",
			Help: "Failed DLT publishes, retried.",
		}),
		Latency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "consumer_event_latency_seconds",
			Help:    "From produced_at to sink write.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 15),
		}),
		Assigned: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "consumer_partition_owner",
			Help: "Consumer number while the partition is assigned to it, 0 otherwise.",
		}, []string{"partition"}),
		Rebalances: f.NewCounterVec(prometheus.CounterOpts{
			Name: "consumer_rebalance_events_total",
			Help: "Partition assignment callbacks.",
		}, []string{"kind"}),
		Commits: f.NewCounterVec(prometheus.CounterOpts{
			Name: "consumer_commits_total",
			Help: "Offset commits by result.",
		}, []string{"result"}),
	}
}

// InitEvents заводит нулевые серии consumer_events_total для каждой партиции и каждого исхода.
// Повторы после ребаланса приходят пачкой быстрее интервала скрейпа: серия, которая появилась
// сразу со значением N, для increase() не растёт, и дубли на дашборде не видны.
func (m *Metrics) InitEvents(partitions []int32) {
	for _, p := range partitions {
		for _, o := range []Outcome{OutcomeOK, OutcomeDuplicate, OutcomeDLT} {
			m.Events.WithLabelValues(strconv.Itoa(int(p)), string(o)).Add(0)
		}
	}
}

// Processor обрабатывает одну запись.
type Processor struct {
	ConsumerID  string
	Delay       time.Duration
	MaxAttempts int
	// RetryBackoff - пауза между повторами записи в sink или DLT, когда они недоступны.
	RetryBackoff time.Duration
	Sink         Sink
	DLT          DLT
	DLTTopic     string
	Metrics      *Metrics
	Log          *slog.Logger
}

// Handle обрабатывает запись: до MaxAttempts попыток, затем DLT. Ошибка только при отмене
// ctx - тогда запись не обработана и коммитить её нельзя.
func (p *Processor) Handle(ctx context.Context, r *kgo.Record) (Outcome, error) {
	var (
		ev  event.Event
		err error
	)
	attempts := 0
	for attempts < p.MaxAttempts {
		attempts++
		if err = sleep(ctx, p.Delay); err != nil {
			return "", err
		}
		if ev, err = event.Decode(r.Value); err == nil {
			break
		}
		p.Metrics.AttemptFailures.WithLabelValues(reason(err)).Inc()
		p.Log.Warn("processing failed", "partition", r.Partition, "offset", r.Offset,
			"attempt", attempts, "err", err)
	}
	part := strconv.Itoa(int(r.Partition))

	if err != nil {
		dlt := DLTRecord(r, p.DLTTopic, err, attempts, p.ConsumerID)
		if err := p.retry(ctx, p.Metrics.DLTErrors, "dlt publish", func() error {
			return p.DLT.Publish(ctx, dlt)
		}); err != nil {
			return "", err
		}
		p.Metrics.Events.WithLabelValues(part, string(OutcomeDLT)).Inc()
		p.Log.Info("sent to dlt", "partition", r.Partition, "offset", r.Offset,
			"key", string(r.Key), "attempts", attempts, "err", err)
		return OutcomeDLT, nil
	}

	row := sink.Row{
		EventID: ev.EventID, UserID: ev.UserID, Seq: ev.Seq, Type: string(ev.Type),
		Poison: ev.Payload.Poison, ProducerID: ev.Payload.Producer, ConsumerID: p.ConsumerID,
		Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, ProducedAt: ev.ProducedAt,
	}
	var dup bool
	if err := p.retry(ctx, p.Metrics.SinkErrors, "sink insert", func() error {
		var err error
		dup, err = p.Sink.Insert(ctx, row)
		return err
	}); err != nil {
		return "", err
	}
	if dup {
		p.Metrics.Events.WithLabelValues(part, string(OutcomeDuplicate)).Inc()
		p.Log.Info("duplicate dropped", "event_id", ev.EventID, "partition", r.Partition, "offset", r.Offset)
		return OutcomeDuplicate, nil
	}
	p.Metrics.Events.WithLabelValues(part, string(OutcomeOK)).Inc()
	p.Metrics.Latency.Observe(time.Since(ev.ProducedAt).Seconds())
	return OutcomeOK, nil
}

// retry повторяет fn до успеха или отмены ctx: недоступный sink или DLT - не повод
// терять запись или коммитить её необработанной.
func (p *Processor) retry(ctx context.Context, errs prometheus.Counter, what string, fn func() error) error {
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		errs.Inc()
		p.Log.Error(what, "err", err)
		if err := sleep(ctx, p.RetryBackoff); err != nil {
			return err
		}
	}
}

func reason(err error) string {
	if errors.Is(err, event.ErrPoison) {
		return "poison"
	}
	return "decode"
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
