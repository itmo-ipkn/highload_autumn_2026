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
)

type Metrics struct {
	registry   *prometheus.Registry
	processed  *prometheus.CounterVec
	duplicates *prometheus.CounterVec
	dlt        *prometheus.CounterVec
	retries    prometheus.Counter
	rebalances prometheus.Counter
	assigned   prometheus.Gauge
	offset     *prometheus.GaugeVec
	latency    prometheus.Histogram
}

func New(podID string) *Metrics {
	reg := prometheus.NewRegistry()
	pod := prometheus.WrapRegistererWith(prometheus.Labels{"pod": podID}, reg)
	m := &Metrics{
		registry:   reg,
		processed:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "consumer_events_processed_total", Help: "Events successfully handled by consumer."}, []string{"topic"}),
		duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "consumer_duplicate_deliveries_total", Help: "Already processed event deliveries."}, []string{"topic"}),
		dlt:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "consumer_events_dlt_total", Help: "Events sent to the dead-letter topic."}, []string{"topic"}),
		retries:    prometheus.NewCounter(prometheus.CounterOpts{Name: "consumer_processing_retries_total", Help: "Processing retries after the first attempt."}),
		rebalances: prometheus.NewCounter(prometheus.CounterOpts{Name: "consumer_group_setups_total", Help: "Consumer group assignment setups."}),
		assigned:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "consumer_assigned_partitions", Help: "Partitions currently assigned to this consumer."}),
		offset:     prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "consumer_partition_offset", Help: "Latest successfully handled Kafka offset."}, []string{"topic", "partition"}),
		latency:    prometheus.NewHistogram(prometheus.HistogramOpts{Name: "consumer_processing_duration_seconds", Help: "Event processing duration including retries.", Buckets: prometheus.DefBuckets}),
	}
	pod.MustRegister(m.processed, m.duplicates, m.dlt, m.retries, m.rebalances, m.assigned, m.offset, m.latency)
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

func (m *Metrics) Processed(topic string, partition int32, offset int64, duplicate bool, elapsed time.Duration) {
	m.processed.WithLabelValues(topic).Inc()
	if duplicate {
		m.duplicates.WithLabelValues(topic).Inc()
	}
	m.offset.WithLabelValues(topic, strconv.Itoa(int(partition))).Set(float64(offset))
	m.latency.Observe(elapsed.Seconds())
}
func (m *Metrics) Duplicate(topic string) { m.duplicates.WithLabelValues(topic).Inc() }
func (m *Metrics) Retried()               { m.retries.Inc() }
func (m *Metrics) DLT(topic string)       { m.dlt.WithLabelValues(topic).Inc() }
func (m *Metrics) Setup(partitions int)   { m.rebalances.Inc(); m.assigned.Set(float64(partitions)) }
func (m *Metrics) Cleanup()               { m.assigned.Set(0) }
func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}
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
