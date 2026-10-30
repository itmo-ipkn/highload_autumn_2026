package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	RequestDuration *prometheus.HistogramVec
	RequestTotal    *prometheus.CounterVec
	ErrorsTotal     *prometheus.CounterVec
}

func NewMetrics() *Metrics {
	return &Metrics{
		RequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_server_request_duration_seconds",
				Help:    "Duration of HTTP requests",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"pod_id", "datacenter", "method", "route", "operations"},
		),

		RequestTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_server_requests_total",
				Help: "Total number of HTTP requests",
			},
			[]string{"pod_id", "datacenter", "method", "route", "operations", "status_code"},
		),

		ErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_server_errors_total",
				Help: "Total number of HTTP 4xx and 5xx",
			},
			[]string{"pod_id", "datacenter", "method", "route","operations", "status_code"},
		),
	}
}

func (m *Metrics) Register(reg prometheus.Registerer) error {
	if err := reg.Register(m.RequestDuration); err != nil {
		return err
	}
	if err := reg.Register(m.RequestTotal); err != nil {
		return err
	}
	if err := reg.Register(m.ErrorsTotal); err != nil {
		return err
	}

	return nil
}

func (m *Metrics) Middleware() gin.HandlerFunc {
	podID := os.Getenv("POD_ID")
	if podID == "" {
		podID = "unknown"
	}

	datacenter := os.Getenv("DATACENTER")
	if datacenter == "" {
		datacenter = "unknown"
	}

	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		fmt.Println("Middleware called:", c.Request.Method, c.Request.URL.Path)

		if c.Request.URL.Path == "/health" ||
			c.Request.URL.Path == "/metrics" ||
			c.Request.URL.Path == "/handlers" {
			return
		}

		statusCode := c.Writer.Status()
		method := c.Request.Method
		route := c.FullPath()

		if route == "" {
			route = "unmatched"
		}

		operations := method + " " + route

		labels := prometheus.Labels{
			"pod_id":     podID,
			"datacenter": datacenter,
			"method":     method,
			"route":      route,
			"operations": operations,
		}

		m.RequestDuration.With(labels).Observe(
			time.Since(start).Seconds(),
		)

		requestLabels := prometheus.Labels{
			"pod_id":      podID,
			"datacenter":  datacenter,
			"method":      method,
			"route":       route,
			"status_code": strconv.Itoa(statusCode),
			"operations": operations,
		}

		m.RequestTotal.With(requestLabels).Inc()

		if statusCode >= 400 && statusCode < 600 {
			m.ErrorsTotal.With(requestLabels).Inc()
		}
	}
}
