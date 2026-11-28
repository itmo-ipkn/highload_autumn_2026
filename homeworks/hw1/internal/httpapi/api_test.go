package httpapi_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"highload/lab1/internal/httpapi"
	"highload/lab1/internal/item"
	"highload/lab1/pkg/monitoring"
)

func TestItemCRUD(t *testing.T) {
	t.Parallel()

	api, _ := readyAPI(t, 1024)
	handler := api.Handler()

	created := requestItem(t, handler, http.MethodPost, "/items", `{"name":"example","value":"some value"}`, http.StatusCreated)
	if created.Name != "example" || created.Value != "some value" || created.ID == "" {
		t.Fatalf("created = %+v", created)
	}

	got := requestItem(t, handler, http.MethodGet, "/items/"+created.ID, "", http.StatusOK)
	if got != created {
		t.Fatalf("get = %+v, created %+v", got, created)
	}

	updated := requestItem(t, handler, http.MethodPut, "/items/"+created.ID, `{"name":"renamed","value":"new value"}`, http.StatusOK)
	if updated.ID != created.ID || updated.Name != "renamed" || updated.Value != "new value" {
		t.Fatalf("update = %+v", updated)
	}

	got = requestItem(t, handler, http.MethodGet, "/items/"+created.ID, "", http.StatusOK)
	if got != updated {
		t.Fatalf("get after update = %+v, updated %+v", got, updated)
	}
}

func TestInvalidInput(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		body   string
		header string
	}{
		{name: "broken json", method: http.MethodPost, path: "/items", body: "{", header: "application/json"},
		{name: "missing value", method: http.MethodPost, path: "/items", body: `{"name":"example"}`, header: "application/json"},
		{name: "empty name", method: http.MethodPost, path: "/items", body: `{"name":"","value":"x"}`, header: "application/json"},
		{name: "wrong type", method: http.MethodPost, path: "/items", body: `{"name":1,"value":"x"}`, header: "application/json"},
		{name: "client id", method: http.MethodPost, path: "/items", body: `{"id":"client","name":"example","value":"x"}`, header: "application/json"},
		{name: "two documents", method: http.MethodPost, path: "/items", body: `{"name":"a","value":"b"}{"name":"c","value":"d"}`, header: "application/json"},
		{name: "text body", method: http.MethodPost, path: "/items", body: `{"name":"a","value":"b"}`, header: "text/plain"},
		{name: "missing content type", method: http.MethodPost, path: "/items", body: `{"name":"a","value":"b"}`},
		{name: "put broken json", method: http.MethodPut, path: "/items/missing", body: "{", header: "application/json"},
		{name: "put empty value", method: http.MethodPut, path: "/items/missing", body: `{"name":"a","value":""}`, header: "application/json"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			api, _ := readyAPI(t, 1024)
			rec := perform(api.Handler(), test.method, test.path, test.body, test.header)
			assertError(t, rec, http.StatusBadRequest, "invalid request")
		})
	}
}

func TestMissingItem(t *testing.T) {
	t.Parallel()

	api, _ := readyAPI(t, 1024)
	handler := api.Handler()

	rec := perform(handler, http.MethodGet, "/items/missing", "", "")
	assertError(t, rec, http.StatusNotFound, "item not found")

	rec = perform(handler, http.MethodPut, "/items/missing", `{"name":"a","value":"b"}`, "application/json")
	assertError(t, rec, http.StatusNotFound, "item not found")

	rec = perform(handler, http.MethodGet, "/items/missing", "", "")
	assertError(t, rec, http.StatusNotFound, "item not found")
}

func TestCharsetAndBodyLimit(t *testing.T) {
	t.Parallel()

	api, _ := readyAPI(t, 48)
	handler := api.Handler()

	created := requestItem(t, handler, http.MethodPost, "/items", `{"name":"a","value":"b"}`, http.StatusCreated)
	if created.Name != "a" {
		t.Fatalf("created = %+v", created)
	}
	rec := perform(handler, http.MethodPost, "/items", `{"name":"a","value":"b"}`, "application/json; charset=utf-8")
	if rec.Code != http.StatusCreated {
		t.Fatalf("charset request status = %d, body %s", rec.Code, rec.Body.String())
	}

	oversized := `{"name":"` + strings.Repeat("n", 80) + `","value":"v"}`
	rec = perform(handler, http.MethodPost, "/items", oversized, "application/json")
	assertError(t, rec, http.StatusBadRequest, "invalid request")
	if strings.Contains(rec.Body.String(), "too large") {
		t.Fatalf("body limit leaked internal text: %s", rec.Body.String())
	}
}

func TestListAndDeleteAreAbsent(t *testing.T) {
	t.Parallel()

	api, _ := readyAPI(t, 1024)
	handler := api.Handler()
	created := requestItem(t, handler, http.MethodPost, "/items", `{"name":"a","value":"b"}`, http.StatusCreated)

	rec := perform(handler, http.MethodGet, "/items", "", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /items status = %d", rec.Code)
	}
	rec = perform(handler, http.MethodDelete, "/items/"+created.ID, "", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE status = %d", rec.Code)
	}
	got := requestItem(t, handler, http.MethodGet, "/items/"+created.ID, "", http.StatusOK)
	if got != created {
		t.Fatalf("item changed after rejected delete: %+v", got)
	}
}

func TestHealthReadiness(t *testing.T) {
	t.Parallel()

	store := item.NewStore()
	health := httpapi.NewHealth()
	api, err := httpapi.New(store, health, 1024, testMetrics(t))
	if err != nil {
		t.Fatalf("new api: %v", err)
	}
	handler := api.Handler()

	rec := perform(handler, http.MethodGet, "/health", "", "")
	assertError(t, rec, http.StatusServiceUnavailable, "not ready")

	health.SetReady(true)
	rec = perform(handler, http.MethodGet, "/health", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("ready status = %d", rec.Code)
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("content type = %q", rec.Header().Get("Content-Type"))
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("health json: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("health body = %+v", body)
	}

	health.SetReady(false)
	rec = perform(handler, http.MethodGet, "/health", "", "")
	assertError(t, rec, http.StatusServiceUnavailable, "not ready")
}

func testMetrics(t *testing.T) *monitoring.Metrics {
	t.Helper()
	metrics, err := monitoring.NewMetrics(monitoring.NewRegistry(), "pod-test", "dc-test")
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	return metrics
}

func readyAPI(t *testing.T, maxBody int64) (*httpapi.API, *item.Store) {
	t.Helper()
	store := item.NewStore()
	health := httpapi.NewHealth()
	health.SetReady(true)
	api, err := httpapi.New(store, health, maxBody, testMetrics(t))
	if err != nil {
		t.Fatalf("new api: %v", err)
	}
	return api, store
}

func requestItem(t *testing.T, handler http.Handler, method, path, body string, want int) item.Item {
	t.Helper()
	header := ""
	if body != "" {
		header = "application/json"
	}
	rec := perform(handler, method, path, body, header)
	if rec.Code != want {
		t.Fatalf("%s %s status = %d, body %s", method, path, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("content type = %q", rec.Header().Get("Content-Type"))
	}
	var got item.Item
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode item: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode object: %v", err)
	}
	if len(raw) != 3 || raw["id"] == "" || raw["name"] == "" || raw["value"] == "" {
		t.Fatalf("response fields = %+v", raw)
	}
	return got
}

func perform(handler http.Handler, method, path, body, contentType string) *httptest.ResponseRecorder {
	var reader io.Reader
	if body != "" {
		reader = bytes.NewBufferString(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func assertError(t *testing.T, rec *httptest.ResponseRecorder, status int, message string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("content type = %q", rec.Header().Get("Content-Type"))
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(body) != 1 || body["error"] != message {
		t.Fatalf("error body = %+v", body)
	}
}
