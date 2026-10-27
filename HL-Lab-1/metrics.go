package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	podID    string
	dc       string
}

func NewMetrics(podID, datacenter string, routes []route) *Metrics {
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		registry: registry,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_server_requests_total",
			Help: "Total number of HTTP requests handled by application routes.",
		}, []string{"handler", "pod_id", "datacenter", "status_code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_server_request_duration_seconds",
			Help:    "HTTP request duration in seconds for application routes.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"handler", "pod_id", "datacenter", "status_code"}),
		podID: podID,
		dc:    datacenter,
	}
	registry.MustRegister(metrics.requests, metrics.duration)

	for _, item := range routes {
		labels := metrics.labels(item.info, http.StatusOK)
		metrics.requests.With(labels).Add(0)
		metrics.duration.With(labels)
	}
	return metrics
}

func (m *Metrics) instrument(item route, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := &responseRecorder{ResponseWriter: w}
		start := time.Now()
		next.ServeHTTP(recorder, r)
		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		labels := m.labels(item.info, status)
		m.requests.With(labels).Inc()
		m.duration.With(labels).Observe(time.Since(start).Seconds())
	})
}

func (m *Metrics) labels(handler handlerInfo, status int) prometheus.Labels {
	return prometheus.Labels{
		"handler":     handler.Method + " " + handler.Path,
		"pod_id":      m.podID,
		"datacenter":  m.dc,
		"status_code": strconv.Itoa(status),
	}
}

func (m *Metrics) handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (w *responseRecorder) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseRecorder) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}
