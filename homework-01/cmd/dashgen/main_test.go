package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fadet/highload-hw1/internal/dashboard"
)

func handlersServer(t *testing.T, handlers ...dashboard.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/handlers" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"handlers": handlers})
	}))
	t.Cleanup(srv.Close)
	return srv
}

type grafanaStub struct {
	*httptest.Server
	received  atomic.Pointer[map[string]any]
	failFirst atomic.Int32
}

func newGrafanaStub(t *testing.T) *grafanaStub {
	t.Helper()
	g := &grafanaStub{}
	g.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if g.failFirst.Add(-1) >= 0 {
			http.Error(w, "warming up", http.StatusServiceUnavailable)
			return
		}
		if user, pass, ok := r.BasicAuth(); !ok || user != "admin" || pass != "secret" {
			http.Error(w, `{"message":"invalid username or password"}`, http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/api/dashboards/db" {
			http.NotFound(w, r)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		g.received.Store(&body)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "success", "url": "/d/items-service/x"})
	}))
	t.Cleanup(g.Close)
	return g
}

func testConfig(grafanaURL string, appURLs ...string) config {
	return config{
		appURLs:         appURLs,
		grafanaURL:      grafanaURL,
		grafanaUser:     "admin",
		grafanaPassword: "secret",
		datasourceUID:   "prometheus",
		dashboardUID:    "items-service",
		dashboardTitle:  "Items",
		timeout:         10 * time.Second,
	}
}

func TestFetchAllHandlersMergesAndDeduplicates(t *testing.T) {
	a := handlersServer(t, dashboard.Handler{Method: "POST", Route: "/items"}, dashboard.Handler{Method: "GET", Route: "/items/{id}"})
	b := handlersServer(t, dashboard.Handler{Method: "GET", Route: "/items/{id}"}, dashboard.Handler{Method: "DELETE", Route: "/items/{id}"})

	got, err := fetchAllHandlers(context.Background(), http.DefaultClient, []string{a.URL, b.URL})
	if err != nil {
		t.Fatal(err)
	}
	want := []dashboard.Handler{
		{Method: "POST", Route: "/items"},
		{Method: "GET", Route: "/items/{id}"},
		{Method: "DELETE", Route: "/items/{id}"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestFetchAllHandlersEmpty(t *testing.T) {
	a := handlersServer(t)
	if _, err := fetchAllHandlers(context.Background(), http.DefaultClient, []string{a.URL}); err == nil {
		t.Fatal("expected error for empty handler list")
	}
}

func TestRunUploadsNonEditableDashboard(t *testing.T) {
	app := handlersServer(t, dashboard.Handler{Method: "GET", Route: "/items/{id}"})
	g := newGrafanaStub(t)
	g.failFirst.Store(2)

	if err := run(context.Background(), testConfig(g.URL, app.URL), http.DefaultClient); err != nil {
		t.Fatal(err)
	}

	body := g.received.Load()
	if body == nil {
		t.Fatal("grafana received nothing")
	}
	if (*body)["overwrite"] != true {
		t.Errorf("overwrite = %v", (*body)["overwrite"])
	}
	d := (*body)["dashboard"].(map[string]any)
	if d["uid"] != "items-service" || d["editable"] != false {
		t.Errorf("uid=%v editable=%v", d["uid"], d["editable"])
	}
	if n := len(d["panels"].([]any)); n != 4 {
		t.Errorf("panels = %d, want 4", n)
	}
}

func TestRunFailsFastOnBadCredentials(t *testing.T) {
	app := handlersServer(t, dashboard.Handler{Method: "GET", Route: "/x"})
	g := newGrafanaStub(t)

	cfg := testConfig(g.URL, app.URL)
	cfg.grafanaPassword = "wrong"

	start := time.Now()
	err := run(context.Background(), cfg, http.DefaultClient)

	var perm permanentError
	if !errors.As(err, &perm) {
		t.Fatalf("expected permanent error, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("4xx must not be retried")
	}
}

func TestRetryGivesUpOnContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_, err := fetchAllHandlers(ctx, http.DefaultClient, []string{"http://127.0.0.1:1"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}
