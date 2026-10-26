package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DenisBorzenkov/ddia-2026/hw-1/internal/metrics"
)

func get(t *testing.T, h http.Handler, method, path, body string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	b, _ := io.ReadAll(rec.Body)
	return rec.Code, string(b)
}

func newServer(t *testing.T, cfg Config) http.Handler {
	t.Helper()
	if cfg.PodID == "" {
		cfg = Config{PodID: "pod-1", Datacenter: "dc-a"}
	}
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestMetricsLabels(t *testing.T) {
	h := newServer(t, Config{})
	get(t, h, http.MethodPost, "/items", `{"name":"a"}`)
	get(t, h, http.MethodGet, "/items/1", "")
	get(t, h, http.MethodGet, "/items/2", "")
	get(t, h, http.MethodGet, "/health", "")
	get(t, h, http.MethodGet, "/nope", "")

	_, body := get(t, h, http.MethodGet, "/metrics", "")
	want := []string{
		`http_server_requests_total{code="201",datacenter="dc-a",method="POST",pod="pod-1",route="/items"} 1`,
		`http_server_requests_total{code="200",datacenter="dc-a",method="GET",pod="pod-1",route="/items/{id}"} 1`,
		`http_server_requests_total{code="404",datacenter="dc-a",method="GET",pod="pod-1",route="/items/{id}"} 1`,
		`http_server_request_duration_seconds_bucket{code="200",datacenter="dc-a",method="GET",pod="pod-1",route="/items/{id}",le="+Inf"} 1`,
	}
	for _, w := range want {
		if !strings.Contains(body, w) {
			t.Errorf("missing %s", w)
		}
	}
	for _, bad := range []string{`route="/health"`, `route="/metrics"`, `route="/nope"`, `route="/items/1"`} {
		if strings.Contains(body, bad) {
			t.Errorf("unexpected %s", bad)
		}
	}
}

func TestHandlers(t *testing.T) {
	code, body := get(t, newServer(t, Config{}), http.MethodGet, "/handlers", "")
	if code != http.StatusOK {
		t.Fatalf("code %d", code)
	}
	var got struct{ Handlers []metrics.Route }
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	want := []metrics.Route{
		{Method: "POST", Route: "/items"},
		{Method: "GET", Route: "/items/{id}"},
		{Method: "PUT", Route: "/items/{id}"},
	}
	if len(got.Handlers) != len(want) {
		t.Fatalf("got %+v", got.Handlers)
	}
	for i := range want {
		if got.Handlers[i] != want[i] {
			t.Errorf("handler %d: got %+v, want %+v", i, got.Handlers[i], want[i])
		}
	}
}

func TestFailRate(t *testing.T) {
	h := newServer(t, Config{PodID: "p", Datacenter: "d", FailRate: 1})
	if code, _ := get(t, h, http.MethodPost, "/items", `{}`); code != http.StatusInternalServerError {
		t.Fatalf("code %d", code)
	}
	if code, _ := get(t, h, http.MethodGet, "/health", ""); code != http.StatusOK {
		t.Fatalf("health %d", code)
	}
	_, body := get(t, h, http.MethodGet, "/metrics", "")
	if !strings.Contains(body, `code="500"`) {
		t.Error("500 not counted")
	}
}
