package env

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registerer - регистратор метрик с лейблом pod на всех сериях.
func Registerer(pod string) prometheus.Registerer {
	return prometheus.WrapRegistererWith(prometheus.Labels{"pod": pod}, prometheus.DefaultRegisterer)
}

// ServeMetrics отдаёт /metrics в фоне.
func ServeMetrics(addr string, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Error("metrics server", "err", err)
		}
	}()
}
