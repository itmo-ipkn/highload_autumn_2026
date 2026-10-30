package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Fadet/highload-hw1/internal/store"
)

func newTestMux() *http.ServeMux {
	mux := http.NewServeMux()
	New(store.New(), Options{}).Register(mux)
	mux.HandleFunc("GET /health", Health)
	return mux
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestItemsLifecycle(t *testing.T) {
	mux := newTestMux()

	rec := do(t, mux, http.MethodPost, "/items", `{"name":"book","value":"42"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST: got %d, body %s", rec.Code, rec.Body)
	}
	var created store.Item
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	if created.ID != 1 || created.Name != "book" {
		t.Fatalf("unexpected created item %+v", created)
	}

	if rec := do(t, mux, http.MethodGet, "/items/1", ""); rec.Code != http.StatusOK {
		t.Fatalf("GET: got %d", rec.Code)
	}

	rec = do(t, mux, http.MethodPut, "/items/1", `{"name":"pen","value":"7"}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"pen"`) {
		t.Fatalf("PUT: got %d, body %s", rec.Code, rec.Body)
	}
}

func TestErrors(t *testing.T) {
	mux := newTestMux()

	tests := []struct {
		name, method, path, body string
		want                     int
	}{
		{"get missing", http.MethodGet, "/items/999", "", http.StatusNotFound},
		{"put missing", http.MethodPut, "/items/999", `{"name":"x"}`, http.StatusNotFound},
		{"bad id", http.MethodGet, "/items/abc", "", http.StatusBadRequest},
		{"bad json", http.MethodPost, "/items", `{`, http.StatusBadRequest},
		{"empty name", http.MethodPost, "/items", `{"value":"1"}`, http.StatusBadRequest},
		{"wrong method", http.MethodDelete, "/items/1", "", http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if rec := do(t, mux, tt.method, tt.path, tt.body); rec.Code != tt.want {
				t.Fatalf("got %d, want %d", rec.Code, tt.want)
			}
		})
	}
}

func TestChaosFailureRate(t *testing.T) {
	tests := []struct {
		rate float64
		want int
	}{
		{1, http.StatusInternalServerError},
		{0, http.StatusCreated},
	}
	for _, tt := range tests {
		mux := http.NewServeMux()
		New(store.New(), Options{FailureRate: tt.rate}).Register(mux)
		for range 20 {
			if rec := do(t, mux, http.MethodPost, "/items", `{"name":"x"}`); rec.Code != tt.want {
				t.Fatalf("rate %v: got %d, want %d", tt.rate, rec.Code, tt.want)
			}
		}
	}
}

func TestChaosLatency(t *testing.T) {
	mux := http.NewServeMux()
	New(store.New(), Options{LatencyMean: 5 * time.Millisecond}).Register(mux)

	const n = 50
	start := time.Now()
	for range n {
		do(t, mux, http.MethodPost, "/items", `{"name":"x"}`)
	}
	if avg := time.Since(start) / n; avg < time.Millisecond {
		t.Fatalf("average latency %v, expected injected delay", avg)
	}
}

func TestHealth(t *testing.T) {
	if rec := do(t, newTestMux(), http.MethodGet, "/health", ""); rec.Code != http.StatusOK {
		t.Fatalf("got %d", rec.Code)
	}
}
