package metrics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	LabelMethod = "method"
	LabelRoute  = "route"
	LabelCode   = "code"
)

const (
	RequestsTotalName   = "http_requests_total"
	RequestDurationName = "http_request_duration_seconds"
)

var DurationBuckets = prometheus.ExponentialBuckets(0.0005, 2, 14)

type Handler struct {
	Method string `json:"method"`
	Route  string `json:"route"`
}

type Router struct {
	mux      *http.ServeMux
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec

	mu       sync.RWMutex
	handlers []Handler
}

func NewRouter(reg prometheus.Registerer, mux *http.ServeMux) *Router {
	r := &Router{
		mux: mux,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: RequestsTotalName,
			Help: "Total number of HTTP requests by handler and response code.",
		}, []string{LabelMethod, LabelRoute, LabelCode}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    RequestDurationName,
			Help:    "HTTP request latency in seconds by handler.",
			Buckets: DurationBuckets,
		}, []string{LabelMethod, LabelRoute}),
	}
	reg.MustRegister(r.requests, r.duration)
	return r
}

func (r *Router) Handle(pattern string, h http.Handler) {
	method, route, ok := strings.Cut(pattern, " ")
	if !ok || method == "" || !strings.HasPrefix(route, "/") {
		panic(fmt.Sprintf("metrics: pattern %q must have the form \"METHOD /path\"", pattern))
	}

	r.duration.WithLabelValues(method, route)

	r.mux.Handle(pattern, r.instrument(method, route, h))

	r.mu.Lock()
	r.handlers = append(r.handlers, Handler{Method: method, Route: route})
	r.mu.Unlock()
}

func (r *Router) Handlers() []Handler {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]Handler(nil), r.handlers...)
}

func (r *Router) HandlersHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string][]Handler{"handlers": r.Handlers()})
	})
}

func (r *Router) instrument(method, route string, next http.Handler) http.Handler {
	duration := r.duration.WithLabelValues(method, route)
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, req)

		duration.Observe(time.Since(start).Seconds())
		r.requests.WithLabelValues(method, route, strconv.Itoa(rec.status)).Inc()
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	s.wroteHeader = true
	return s.ResponseWriter.Write(b)
}

func (s *statusRecorder) Unwrap() http.ResponseWriter {
	return s.ResponseWriter
}
