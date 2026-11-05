package monitoring

import (
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Config struct {
	Pod string
	DC  string
}

type HandlerInfo struct {
	Method  string `json:"method"`
	Path    string `json:"path"`
	Handler string `json:"handler"`
}

type Monitor struct {
	mux      *http.ServeMux
	registry *prometheus.Registry
	duration *prometheus.HistogramVec
	requests *prometheus.CounterVec

	mu       sync.RWMutex
	handlers []HandlerInfo
}

var durationBuckets = prometheus.ExponentialBucketsRange(0.0005, 5, 20)

func New(mux *http.ServeMux, cfg Config) *Monitor {
	constLabels := prometheus.Labels{"pod": cfg.Pod, "dc": cfg.DC}

	m := &Monitor{
		mux:      mux,
		registry: prometheus.NewRegistry(),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "http_request_duration_seconds",
			Help:        "HTTP request latency by handler.",
			Buckets:     durationBuckets,
			ConstLabels: constLabels,
		}, []string{"handler"}),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "http_requests_total",
			Help:        "HTTP requests by handler and response code.",
			ConstLabels: constLabels,
		}, []string{"handler", "code"}),
	}
	m.registry.MustRegister(
		m.duration,
		m.requests,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

func (m *Monitor) Handle(pattern string, h http.Handler) {
	method, path, _ := strings.Cut(pattern, " ")

	m.mu.Lock()
	m.handlers = append(m.handlers, HandlerInfo{Method: method, Path: path, Handler: pattern})
	m.mu.Unlock()

	m.duration.WithLabelValues(pattern)

	m.mux.Handle(pattern, m.instrument(pattern, h))
}

func (m *Monitor) instrument(pattern string, next http.Handler) http.Handler {
	duration := m.duration.WithLabelValues(pattern)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		duration.Observe(time.Since(start).Seconds())
		m.requests.WithLabelValues(pattern, strconv.Itoa(rec.code)).Inc()
	})
}

func (m *Monitor) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry})
}

func (m *Monitor) HandlersHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(m.Handlers())
	})
}

func (m *Monitor) Handlers() []HandlerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Clone(m.handlers)
}

type statusRecorder struct {
	http.ResponseWriter
	code        int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.code = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}
