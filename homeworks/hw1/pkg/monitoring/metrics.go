package monitoring

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// RequestsTotal counts responses of instrumented routes.
	RequestsTotal = "http_requests_total"
	// RequestDurationSeconds is the handler latency histogram.
	// Prometheus publishes its buckets under RequestDurationBucket.
	RequestDurationSeconds = "http_request_duration_seconds"
	RequestDurationBucket  = RequestDurationSeconds + "_bucket"
)

// Metrics collects request counts and latency for routes registered with a Registrar.
// pod and datacenter are fixed for the process and are not read from the environment here.
type Metrics struct {
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func NewMetrics(reg *prometheus.Registry, podID, datacenter string) (*Metrics, error) {
	if reg == nil {
		return nil, errors.New("registry is required")
	}
	if err := ValidateIdentity(podID, datacenter); err != nil {
		return nil, err
	}

	constLabels := prometheus.Labels{
		"pod_id":     podID,
		"datacenter": datacenter,
	}
	metrics := &Metrics{registry: reg}
	metrics.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        RequestsTotal,
		Help:        "HTTP responses served by instrumented routes.",
		ConstLabels: constLabels,
	}, []string{"method", "route", "status_code"})
	metrics.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        RequestDurationSeconds,
		Help:        "HTTP request duration in seconds for instrumented routes.",
		ConstLabels: constLabels,
		Buckets:     DurationBuckets(),
	}, []string{"method", "route"})

	if err := reg.Register(metrics.requests); err != nil {
		return nil, fmt.Errorf("register %s: %w", RequestsTotal, err)
	}
	if err := reg.Register(metrics.duration); err != nil {
		return nil, fmt.Errorf("register %s: %w", RequestDurationSeconds, err)
	}
	return metrics, nil
}

// Handler serves this registry in Prometheus text format.
// It is not itself an instrumented route.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) observe(method, route string, status int, elapsed time.Duration) {
	if elapsed < 0 {
		elapsed = 0
	}
	m.requests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	m.duration.WithLabelValues(method, route).Observe(elapsed.Seconds())
}
