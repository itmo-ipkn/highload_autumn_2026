package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	serviceName = "http-server"
)

var metricLabelNames = []string{
	"pod_id",
	"datacenter",
	"method",
	"path",
	"status_code",
}

type httpServerMetrics struct {
	registry       *prometheus.Registry
	requestLatency *prometheus.HistogramVec
	requestsTotal  *prometheus.CounterVec
	podID          string
	datacenter     string
}

func NewHttpServerMetrics(appName, podID, datacenter string) *httpServerMetrics {
	registry := prometheus.NewRegistry()

	requestLatency := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        "http_server_requests_latency",
			Help:        "Request processing time",
			ConstLabels: prometheus.Labels{"app": appName, "service": serviceName},
			Buckets:     []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		metricLabelNames,
	)

	requestsCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Help:        "Number of processed HTTP requests",
			Name:        "http_server_requests_count",
			ConstLabels: prometheus.Labels{"app": appName, "service": serviceName},
		},
		metricLabelNames,
	)

	registry.MustRegister(requestLatency, requestsCount)

	return &httpServerMetrics{
		registry:       registry,
		requestLatency: requestLatency,
		requestsTotal:  requestsCount,
		podID:          podID,
		datacenter:     datacenter,
	}
}

func (h *httpServerMetrics) Inc(method, path, status_code string) {
	h.requestsTotal.WithLabelValues(h.podID, h.datacenter, method, path, status_code).Inc()
}

func (h *httpServerMetrics) WriteTiming(start time.Time, method, path, status_code string) {
	h.requestLatency.WithLabelValues(h.podID, h.datacenter, method, path, status_code).Observe(time.Since(start).Seconds())
}

func (h *httpServerMetrics) HandleRegistry() http.Handler {
	return promhttp.HandlerFor(h.registry, promhttp.HandlerOpts{})
}
