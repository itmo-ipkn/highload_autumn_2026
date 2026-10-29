// Package monitoring records latency, throughput, and errors for explicitly
// registered HTTP routes and exposes them from a private Prometheus registry.
package monitoring

import "github.com/prometheus/client_golang/prometheus"

func NewRegistry() *prometheus.Registry {
	return prometheus.NewRegistry()
}
