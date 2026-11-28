package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseCatalog(t *testing.T) {
	t.Parallel()

	body := []byte(`{"handlers":[{"method":"POST","path":"/widgets"},{"method":"GET","path":"/widgets/{id}"}]}`)
	handlers, err := ParseCatalog(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(handlers) != 2 || handlers[1].Path != "/widgets/{id}" {
		t.Fatalf("handlers = %+v", handlers)
	}
}

func TestParseCatalogRejectsMalformedPayloads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "empty", body: ""},
		{name: "html", body: "<html>"},
		{name: "array", body: "[]"},
		{name: "missing field", body: `{"other":[]}`},
		{name: "null", body: `{"handlers":null}`},
		{name: "empty list", body: `{"handlers":[]}`},
		{name: "not array", body: `{"handlers":{}}`},
		{name: "extra field", body: `{"handlers":[],"extra":1}`},
		{name: "entry extra", body: `{"handlers":[{"method":"GET","path":"/a","route":"/a"}]}`},
		{name: "lowercase method", body: `{"handlers":[{"method":"get","path":"/a"}]}`},
		{name: "dollar path", body: `{"handlers":[{"method":"GET","path":"/a/$id"}]}`},
		{name: "duplicate", body: `{"handlers":[{"method":"GET","path":"/a"},{"method":"GET","path":"/a"}]}`},
		{name: "two documents", body: `{"handlers":[{"method":"GET","path":"/a"}]}{"handlers":[]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := ParseCatalog([]byte(test.body)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestFetchCatalogUsesHandlersOnly(t *testing.T) {
	t.Parallel()

	var calls []string
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		if r.Header.Get("Authorization") != "" {
			t.Errorf("catalog request included authorization")
		}
		if r.Method != http.MethodGet || r.URL.Path != "/handlers" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"handlers": []map[string]string{{"method": "GET", "path": "/widgets/{id}"}},
		})
	}))
	t.Cleanup(app.Close)

	handlers, err := FetchCatalog(context.Background(), app.Client(), Config{
		AppBaseURL: app.URL,
		GrafanaURL: "http://grafana.example",
		Username:   "admin",
		Password:   "unused",
		Attempts:   1,
		Backoff:    -1,
	})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(handlers) != 1 || strings.Join(calls, ",") != "GET /handlers" {
		t.Fatalf("handlers=%+v calls=%v", handlers, calls)
	}
}

func TestFetchCatalogDoesNotTreatFailureAsEmpty(t *testing.T) {
	t.Parallel()

	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(app.Close)

	_, err := FetchCatalog(context.Background(), app.Client(), Config{
		AppBaseURL: app.URL,
		GrafanaURL: "http://grafana.example",
		Username:   "admin",
		Password:   "unused",
		Attempts:   2,
		Backoff:    -1,
	})
	if err == nil {
		t.Fatal("expected an error")
	}
}
