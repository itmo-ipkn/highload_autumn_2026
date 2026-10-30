package api_test

import (
	"encoding/json"
	"net/http"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"highload-hw1/internal/api"
	"highload-hw1/internal/items"
	"highload-hw1/internal/monitoring"
)

func scrape(t *testing.T, handler http.Handler) map[string]*dto.MetricFamily {
	t.Helper()
	response := request(handler, "GET", "/metrics", "")
	if response.Code != 200 {
		t.Fatalf("metrics: %d", response.Code)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return families
}

func labels(metric *dto.Metric) map[string]string {
	result := make(map[string]string)
	for _, pair := range metric.Label {
		result[pair.GetName()] = pair.GetValue()
	}
	return result
}

func TestMetricsCountResponsesAndNormalizePaths(t *testing.T) {
	handler := api.New(items.NewStore(), "pod-a", "dc-a")
	// Служебные запросы и неизвестные пути не должны попадать в метрики.
	for _, path := range []string{"/health", "/handlers", "/metrics", "/missing"} {
		request(handler, "GET", path, "")
	}
	request(handler, "DELETE", "/items/1", "")
	if got := scrape(t, handler); len(got) != 0 {
		t.Fatalf("unexpected business metrics: %v", got)
	}
	request(handler, "POST", "/items", `{"name":"one"}`)
	request(handler, "GET", "/items/1", "")
	request(handler, "GET", "/items/99", "")
	request(handler, "GET", "/items/100", "")
	request(handler, "PUT", "/items/1", `{"name":"two"}`)
	request(handler, "POST", "/items", `{`)
	families := scrape(t, handler)
	expected := map[string]float64{"POST /items 201": 1, "POST /items 400": 1,
		"GET /items/{id} 200": 1, "GET /items/{id} 404": 2, "PUT /items/{id} 200": 1}
	counter := families["items_api_requests_total"]
	if counter == nil {
		t.Fatal("missing request counter")
	}
	for _, metric := range counter.Metric {
		l := labels(metric)
		if l["pod_id"] != "pod-a" || l["datacenter"] != "dc-a" {
			t.Errorf("wrong identity: %v", l)
		}
		key := l["method"] + " " + l["route"] + " " + l["status_code"]
		want, ok := expected[key]
		if !ok || metric.GetCounter().GetValue() != want {
			t.Errorf("unexpected counter %s: %v", key, metric.GetCounter())
		}
		delete(expected, key)
	}
	if len(expected) != 0 {
		t.Errorf("missing counters: %v", expected)
	}
	histogram := families["items_api_request_duration_seconds"]
	if histogram == nil {
		t.Fatal("missing histogram")
	}
	var observations uint64
	for _, metric := range histogram.Metric {
		observations += metric.GetHistogram().GetSampleCount()
		if len(metric.GetHistogram().Bucket) == 0 {
			t.Error("histogram has no buckets")
		}
	}
	if observations != 6 {
		t.Errorf("observed %d requests, want 6", observations)
	}
	other := api.New(items.NewStore(), "pod-b", "dc-b")
	if len(scrape(t, other)) != 0 {
		t.Error("registries share observations")
	}
}

func TestHandlersDiscoverBusinessRoutes(t *testing.T) {
	handler := api.New(items.NewStore(), "pod", "dc")
	response := request(handler, "GET", "/handlers", "")
	var catalog struct {
		Handlers []monitoring.Handler `json:"handlers"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &catalog); err != nil {
		t.Fatal(err)
	}
	want := []monitoring.Handler{{Method: "POST", Path: "/items"},
		{Method: "GET", Path: "/items/{id}"}, {Method: "PUT", Path: "/items/{id}"}}
	if len(catalog.Handlers) != len(want) {
		t.Fatalf("unexpected catalog: %+v", catalog)
	}
	for i := range want {
		if catalog.Handlers[i] != want[i] {
			t.Errorf("handler %d: %+v", i, catalog.Handlers[i])
		}
	}
}
