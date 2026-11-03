// Package monitoring собирает метрики HTTP-ручек.
package monitoring

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Monitor struct {
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func New(podID, datacenter string) *Monitor {
	labels := prometheus.Labels{"pod_id": podID, "datacenter": datacenter}
	m := &Monitor{
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "items_api_requests_total",
			Help:        "Completed business requests, including unsuccessful responses.",
			ConstLabels: labels,
		}, []string{"method", "route", "status_code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "items_api_request_duration_seconds",
			Help:        "Server-side duration of business requests in seconds.",
			ConstLabels: labels,
			// Для агрегации границы бакетов должны совпадать у всех экземпляров.
			Buckets: []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"method", "route"}),
	}
	// Отдельный реестр изолирует метрики экземпляров и тестов.
	m.registry.MustRegister(m.requests, m.duration)
	return m
}

func (m *Monitor) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Monitor) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		response := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(response, r)
		// Полный шаблон пути читаем после вызова обработчика.
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			return
		} // Для неизвестного пути нет бизнес-ручки.
		status := response.Status()
		if status == 0 {
			status = http.StatusOK
		}
		m.requests.WithLabelValues(r.Method, route, strconv.Itoa(status)).Inc()
		m.duration.WithLabelValues(r.Method, route).Observe(time.Since(started).Seconds())
	})
}
