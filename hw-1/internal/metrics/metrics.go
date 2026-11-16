// Package metrics - золотые сигналы REST-ручек в формате Prometheus.
//
// Middleware вешается на роутер с бизнес-ручками. Служебные ручки
// (/health, /metrics, /handlers) регистрируются вне него и не учитываются.
package metrics

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Buckets подобраны под in-memory ручки: от 1 ms до 1 s.
var Buckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1}

type Metrics struct {
	reg      *prometheus.Registry
	duration *prometheus.HistogramVec
	requests *prometheus.CounterVec
}

// New создаёт метрики; pod и datacenter добавляются ко всем сериям.
func New(pod, datacenter string) *Metrics {
	reg := prometheus.NewRegistry()
	r := prometheus.WrapRegistererWith(prometheus.Labels{"pod": pod, "datacenter": datacenter}, reg)

	m := &Metrics{
		reg: reg,
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_server_request_duration_seconds",
			Help:    "Время обработки запроса.",
			Buckets: Buckets,
		}, []string{"method", "route", "code"}),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_server_requests_total",
			Help: "Число обработанных запросов.",
		}, []string{"method", "route", "code"}),
	}
	r.MustRegister(m.duration, m.requests,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// Middleware считает длительность и коды ответов. Ручка определяется
// шаблоном пути chi (/items/{id}), а не фактическим путём.
func (m *Metrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)

		route := routePattern(r)
		if route == "" {
			return // путь не сматчился ни с одной ручкой
		}
		code := strconv.Itoa(ww.Status())
		m.duration.WithLabelValues(r.Method, route, code).Observe(time.Since(start).Seconds())
		m.requests.WithLabelValues(r.Method, route, code).Inc()
	})
}

// Handler отдаёт метрики для GET /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

func routePattern(r *http.Request) string {
	rctx := chi.RouteContext(r.Context())
	if rctx == nil {
		return ""
	}
	p := rctx.RoutePattern()
	if p == "" || strings.HasSuffix(p, "/*") {
		return ""
	}
	return p
}
