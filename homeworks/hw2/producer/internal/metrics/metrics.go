// Package metrics — Prometheus-метрики продюсера и HTTP-сервер для них.
package metrics

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"highload/hw2/producer/internal/event"
)

// Metrics реализует generator.Metrics и publisher.Metrics.
type Metrics struct {
	registry  *prometheus.Registry
	generated *prometheus.CounterVec
	enqueued  prometheus.Counter
	acked     prometheus.Counter
	failed    *prometheus.CounterVec
	latency   prometheus.Histogram
	inFlight  prometheus.Gauge
}

// New регистрирует метрики в отдельном registry. Метка pod проставляется всем сериям.
func New(podID string) *Metrics {
	reg := prometheus.NewRegistry()
	f := prometheus.WrapRegistererWith(prometheus.Labels{"pod": podID}, reg)

	m := &Metrics{
		registry: reg,
		generated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "producer_events_generated_total",
			Help: "Events generated and acknowledged by Kafka.",
		}, []string{"type", "poison"}),
		enqueued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "producer_messages_enqueued_total",
			Help: "Messages handed to the sarama async producer.",
		}),
		acked: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "producer_messages_acked_total",
			Help: "Messages acknowledged by Kafka (acks=all).",
		}),
		failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "producer_messages_failed_total",
			Help: "Messages that failed after all retries.",
		}, []string{"reason"}),
		latency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "producer_ack_latency_seconds",
			Help:    "Time from enqueue to broker acknowledgement.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		}),
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "producer_in_flight_messages",
			Help: "Messages sent but not yet acknowledged or failed.",
		}),
	}
	f.MustRegister(m.generated, m.enqueued, m.acked, m.failed, m.latency, m.inFlight)
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

func (m *Metrics) EventGenerated(t event.Type, poison bool) {
	m.generated.WithLabelValues(string(t), strconv.FormatBool(poison)).Inc()
}

func (m *Metrics) Enqueued()                   { m.enqueued.Inc() }
func (m *Metrics) Acked(latency time.Duration) { m.acked.Inc(); m.latency.Observe(latency.Seconds()) }
func (m *Metrics) Failed(reason string)        { m.failed.WithLabelValues(reason).Inc() }
func (m *Metrics) SetInFlight(n int)           { m.inFlight.Set(float64(n)) }

// Handler отдаёт /metrics и /healthz.
func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

// Serve запускает HTTP-сервер метрик и останавливает его при отмене ctx.
func Serve(ctx context.Context, addr string, h http.Handler) error {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
