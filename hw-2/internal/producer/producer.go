// Package producer генерирует события с заданной скоростью и пишет их в Kafka.
package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/event"
)

// KeyMode - чем заполнять ключ записи.
type KeyMode string

// Режимы ключа.
const (
	KeyUserID KeyMode = "user_id"
	KeyNone   KeyMode = "none"
)

// ParseKeyMode проверяет значение KEY_MODE.
func ParseKeyMode(s string) (KeyMode, error) {
	switch m := KeyMode(s); m {
	case KeyUserID, KeyNone:
		return m, nil
	default:
		return "", fmt.Errorf("KEY_MODE: %q, want user_id or none", s)
	}
}

// Record собирает запись Kafka из события. С KeyNone ключ пустой, партицию выбирает
// партиционер клиента.
func Record(e event.Event, mode KeyMode) (*kgo.Record, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	r := &kgo.Record{Value: b}
	if mode == KeyUserID {
		r.Key = []byte(e.UserID)
	}
	return r, nil
}

// Client - то, что продюсеру нужно от *kgo.Client.
type Client interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Flush(ctx context.Context) error
	BufferedProduceRecords() int64
}

// Metrics продюсера.
type Metrics struct {
	Generated *prometheus.CounterVec
	Produced  *prometheus.CounterVec
	Failed    prometheus.Counter
	Buffered  prometheus.Gauge
	// BatchErrors - ошибки брокера на produce-батчи по коду, клиент их ретраит.
	BatchErrors *prometheus.CounterVec
}

// NewMetrics регистрирует метрики в reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		Generated: f.NewCounterVec(prometheus.CounterOpts{
			Name: "producer_events_generated_total",
			Help: "Events handed to the Kafka client.",
		}, []string{"poison"}),
		Produced: f.NewCounterVec(prometheus.CounterOpts{
			Name: "producer_events_acked_total",
			Help: "Events acknowledged by Kafka (acks=all).",
		}, []string{"partition"}),
		Failed: f.NewCounter(prometheus.CounterOpts{
			Name: "producer_events_failed_total",
			Help: "Events the client gave up on.",
		}),
		Buffered: f.NewGauge(prometheus.GaugeOpts{
			Name: "producer_buffered_records",
			Help: "Records buffered in the client and not yet acknowledged.",
		}),
		BatchErrors: f.NewCounterVec(prometheus.CounterOpts{
			Name: "producer_batch_errors_total",
			Help: "Broker errors on produce batches, retried by the client.",
		}, []string{"code"}),
	}
}

// Config продюсера.
type Config struct {
	Rate         float64
	KeyMode      KeyMode
	FlushTimeout time.Duration
}

// Stats - счётчики продюсера. Failed дорастает и после Run: записи, не досланные за
// FlushTimeout, клиент отдаёт с ошибкой при Close.
type Stats struct {
	Generated, Acked, Failed atomic.Int64
}

// Run генерирует события, пока ctx не отменён, затем досылает буфер не дольше FlushTimeout.
func Run(ctx context.Context, cfg Config, gen *event.Generator, cl Client, m *Metrics, log *slog.Logger, st *Stats) {
	promise := func(r *kgo.Record, err error) {
		if err != nil {
			st.Failed.Add(1)
			m.Failed.Inc()
			log.Error("produce failed", "key", string(r.Key), "value", string(r.Value), "err", err)
			return
		}
		st.Acked.Add(1)
		m.Produced.WithLabelValues(strconv.Itoa(int(r.Partition))).Inc()
	}

	tick := time.NewTicker(time.Duration(float64(time.Second) / cfg.Rate))
	defer tick.Stop()
	gauge := time.NewTicker(time.Second)
	defer gauge.Stop()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-gauge.C:
			m.Buffered.Set(float64(cl.BufferedProduceRecords()))
		case <-tick.C:
			e := gen.Next()
			r, err := Record(e, cfg.KeyMode)
			if err != nil {
				log.Error("encode", "err", err)
				continue
			}
			st.Generated.Add(1)
			m.Generated.WithLabelValues(strconv.FormatBool(e.Payload.Poison)).Inc()
			// Отдельный контекст: отмена ctx на SIGTERM не должна снимать записи из буфера.
			// При полном буфере (MaxBufferedRecords) Produce блокируется и скорость падает.
			cl.Produce(context.Background(), r, promise)
		}
	}

	log.Info("flushing", "buffered", cl.BufferedProduceRecords())
	fctx, cancel := context.WithTimeout(context.Background(), cfg.FlushTimeout)
	defer cancel()
	if err := cl.Flush(fctx); err != nil {
		log.Error("flush", "err", err, "buffered", cl.BufferedProduceRecords())
	}
}
