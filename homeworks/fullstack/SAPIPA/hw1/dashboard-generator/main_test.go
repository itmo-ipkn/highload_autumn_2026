package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDiscoverNewHandlerWithoutGeneratorChanges(t *testing.T) {
	want := []Handler{
		{Name: "create", Method: "POST", Path: "/items"},
		{Name: "get", Method: "GET", Path: "/items/{id}"},
		{Name: "update", Method: "PUT", Path: "/items/{id}"},
		{Name: "new-handler", Method: "PATCH", Path: "/new-resource/{key}"},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/handlers" || r.Method != http.MethodGet {
			t.Errorf("unexpected discovery request: %s %s", r.Method, r.URL.Path)
		}
		json.NewEncoder(w).Encode(HandlersResponse{Handlers: want})
	}))
	defer server.Close()

	handlers, err := fetchHandlers(server.Client(), server.URL+"/")
	if err != nil {
		t.Fatal(err)
	}
	dash, err := buildDashboard(handlers)
	if err != nil {
		t.Fatal(err)
	}
	if len(dash.Panels) != 3*len(want) {
		t.Fatalf("got %d panels, want %d", len(dash.Panels), 3*len(want))
	}
	body, err := json.Marshal(dash)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "PATCH /new-resource/{key}") {
		t.Fatal("newly discovered handler is missing from dashboard")
	}
}

func TestRejectInvalidDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{name: "HTTP error", status: http.StatusServiceUnavailable, body: "unavailable"},
		{name: "malformed JSON", status: http.StatusOK, body: "{"},
		{name: "trailing JSON", status: http.StatusOK, body: `{"handlers":[]} {}`},
		{name: "empty registry", status: http.StatusOK, body: `{"handlers":[]}`},
		{name: "missing metadata", status: http.StatusOK, body: `{"handlers":[{"method":"GET","path":"/items/{id}"}]}`},
		{name: "duplicate route", status: http.StatusOK, body: `{"handlers":[{"name":"a","method":"GET","path":"/same"},{"name":"b","method":"GET","path":"/same"}]}`},
		{name: "oversized body", status: http.StatusOK, body: strings.Repeat(" ", maxDiscoveryBytes+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			if _, err := fetchHandlers(server.Client(), server.URL); err == nil {
				t.Fatal("invalid discovery response was accepted")
			}
		})
	}
}

func TestOneComponentCanExposeSeveralMethods(t *testing.T) {
	handlers := []Handler{
		{Name: "shared-component", Method: "GET", Path: "/records"},
		{Name: "shared-component", Method: "POST", Path: "/records"},
	}
	dash, err := buildDashboard(handlers)
	if err != nil {
		t.Fatal(err)
	}
	if len(dash.Panels) != 6 {
		t.Fatalf("got %d panels, want 6 for two method/path pairs", len(dash.Panels))
	}
}

func TestDashboardAggregationAndSelectors(t *testing.T) {
	handler := Handler{Name: "get-record", Method: "GET", Path: "/records/{id}"}
	dash, err := buildDashboard([]Handler{handler})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(dash)
	if err != nil {
		t.Fatal(err)
	}

	var actual struct {
		UID      string `json:"uid"`
		Editable bool   `json:"editable"`
		Panels   []struct {
			ID         int
			Datasource struct{ UID string }
			Targets    []struct {
				Expr   string
				Range  bool
				Legend string `json:"legendFormat"`
			}
		}
		Templating struct {
			List []struct {
				Name, Query, AllValue string
				Multi, IncludeAll     bool
				Refresh               int
				Datasource            struct{ UID string }
				Current               struct{ Text, Value string }
			}
		}
	}
	if err := json.Unmarshal(body, &actual); err != nil {
		t.Fatal(err)
	}
	if actual.UID != dashboardUID || actual.Editable {
		t.Fatalf("dashboard must have stable UID and be read-only: %s", body)
	}
	if len(actual.Panels) != 3 || len(actual.Templating.List) != 2 {
		t.Fatalf("expected three panels and two selectors: %s", body)
	}
	for i, variable := range actual.Templating.List {
		if !variable.Multi || !variable.IncludeAll || variable.AllValue != ".*" ||
			variable.Current.Value != "$__all" || variable.Refresh == 0 || variable.Datasource.UID != datasourceUID {
			t.Errorf("selector is not initialized for multi/All: %+v", variable)
		}
		if i == 1 && !strings.Contains(variable.Query, `datacenter=~"${datacenter:regex}"`) {
			t.Error("pod selector must follow selected datacenters")
		}
	}
	if len(actual.Panels[0].Targets) != 3 {
		t.Fatal("latency must contain p50, p95 and p99")
	}
	for i, quantile := range []string{"0.50", "0.95", "0.99"} {
		expr := actual.Panels[0].Targets[i].Expr
		if !strings.HasPrefix(expr, "histogram_quantile("+quantile+", sum by (le) (rate(") ||
			!strings.Contains(expr, `method="GET",route="/records/{id}"`) {
			t.Errorf("latency query must aggregate buckets before quantile: %s", expr)
		}
	}
	if !strings.HasPrefix(actual.Panels[1].Targets[0].Expr, "sum(rate(") {
		t.Error("RPS must sum rates across selected instances")
	}
	errorTarget := actual.Panels[2].Targets[0]
	if !strings.HasPrefix(errorTarget.Expr, "sum by (status) (rate(") ||
		!strings.Contains(errorTarget.Expr, `status=~"4..|5.."`) || errorTarget.Legend != "HTTP {{status}}" {
		t.Errorf("errors must preserve individual response codes: %+v", errorTarget)
	}
	seenIDs := make(map[int]bool)
	for _, panel := range actual.Panels {
		if panel.Datasource.UID != datasourceUID || seenIDs[panel.ID] {
			t.Errorf("incorrect datasource or duplicate panel ID: %+v", panel)
		}
		seenIDs[panel.ID] = true
		for _, target := range panel.Targets {
			if !target.Range || !strings.Contains(target.Expr, `datacenter=~"${datacenter:regex}"`) ||
				!strings.Contains(target.Expr, `pod_id=~"${pod_id:regex}"`) || !strings.Contains(target.Expr, "[$__rate_interval]") {
				t.Errorf("query must support multi/All and adaptive rate intervals: %s", target.Expr)
			}
		}
	}
}

func TestUploadUsesAdminCredentialsAndIdempotentOverwrite(t *testing.T) {
	dash, err := buildDashboard([]Handler{{Name: "example", Method: "GET", Path: "/example"}})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		username, password, ok := r.BasicAuth()
		if !ok || username != "test-admin" || password != "test-password" {
			t.Error("missing configured administrator credentials")
		}
		if r.Method != http.MethodPost || r.URL.Path != "/api/dashboards/db" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("incorrect API request: %s %s", r.Method, r.URL.Path)
		}
		var payload struct {
			Overwrite bool
			Dashboard struct {
				UID      string
				Editable bool
			}
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		if !payload.Overwrite || payload.Dashboard.UID != dashboardUID || payload.Dashboard.Editable {
			t.Errorf("upload must replace the same read-only dashboard: %+v", payload)
		}
		fmt.Fprint(w, `{"status":"success"}`)
	}))
	defer server.Close()
	for i := 0; i < 2; i++ {
		if err := uploadDashboard(server.Client(), server.URL+"/", "test-admin", "test-password", dash); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("got %d uploads, want 2", calls)
	}
}

func TestUploadReportsGrafanaFailure(t *testing.T) {
	dash, err := buildDashboard([]Handler{{Name: "example", Method: "GET", Path: "/example"}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"message":"invalid credentials"}`)
	}))
	defer server.Close()
	err = uploadDashboard(server.Client(), server.URL, "admin", "bad-password", dash)
	if err == nil || !strings.Contains(err.Error(), "401") || !strings.Contains(err.Error(), "invalid credentials") {
		t.Fatalf("expected actionable API failure, got %v", err)
	}
}
