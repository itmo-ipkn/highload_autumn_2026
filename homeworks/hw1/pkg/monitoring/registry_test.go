package monitoring_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"highload/lab1/pkg/monitoring"
)

func TestNewRegistryIsIsolated(t *testing.T) {
	t.Parallel()

	first := monitoring.NewRegistry()
	second := monitoring.NewRegistry()

	for _, registry := range []*prometheus.Registry{first, second} {
		metrics, err := registry.Gather()
		if err != nil {
			t.Fatalf("gather new registry: %v", err)
		}
		if len(metrics) != 0 {
			t.Fatalf("new registry contains %d metric families; want none", len(metrics))
		}
	}

	opts := prometheus.CounterOpts{
		Name: "registry_test_requests_total",
		Help: "Requests used to check registry isolation.",
	}
	if err := first.Register(prometheus.NewCounter(opts)); err != nil {
		t.Fatalf("register in first registry: %v", err)
	}

	metrics, err := second.Gather()
	if err != nil {
		t.Fatalf("gather second registry: %v", err)
	}
	if len(metrics) != 0 {
		t.Fatal("registration in first registry leaked into second registry")
	}
	if err := second.Register(prometheus.NewCounter(opts)); err != nil {
		t.Fatalf("register same metric name in independent registry: %v", err)
	}
}
