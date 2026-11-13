package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testApp struct {
	mu      sync.Mutex
	items   map[uint64]string
	nextID  uint64
	methods map[string]int
}

func (app *testApp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	app.mu.Lock()
	defer app.mu.Unlock()
	app.methods[r.Method]++
	if r.Method == http.MethodPost && r.URL.Path == "/items" {
		var item struct{ Name string }
		if r.Header.Get("Content-Type") != "application/json" || json.NewDecoder(r.Body).Decode(&item) != nil || item.Name == "" {
			http.Error(w, "invalid item", http.StatusBadRequest)
			return
		}
		app.nextID++
		app.items[app.nextID] = item.Name
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"id":%d}`, app.nextID)
		return
	}
	if r.Method == http.MethodGet && r.URL.Query().Get("simulate_error") == "1" {
		http.Error(w, "simulated failure", http.StatusInternalServerError)
		return
	}
	id, err := strconv.ParseUint(strings.TrimPrefix(r.URL.Path, "/items/"), 10, 64)
	if _, exists := app.items[id]; err != nil || !exists {
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodPut {
		var item struct{ Name string }
		if r.Header.Get("Content-Type") != "application/json" || json.NewDecoder(r.Body).Decode(&item) != nil || item.Name == "" {
			http.Error(w, "invalid item", http.StatusBadRequest)
			return
		}
		app.items[id] = item.Name
	}
	fmt.Fprintf(w, `{"id":%d}`, id)
}

func newTestApp(t *testing.T, firstID uint64) (*testApp, *httptest.Server) {
	t.Helper()
	app := &testApp{items: make(map[uint64]string), nextID: firstID, methods: make(map[string]int)}
	server := httptest.NewServer(app)
	t.Cleanup(server.Close)
	return app, server
}

func TestTrafficCoversBothIndependentPodsAndAllHandlers(t *testing.T) {
	ctx := context.Background()
	client := &http.Client{Timeout: time.Second}
	for index := 0; index < 2; index++ {
		app, server := newTestApp(t, uint64(index*1000))
		pod := &Pod{Name: fmt.Sprintf("pod-%d", index), URL: server.URL}
		if status, err := createItem(ctx, client, pod, 0); status != http.StatusCreated || err != nil {
			t.Fatalf("seed: status=%d error=%v", status, err)
		}
		var stats Stats
		for sequence := 0; sequence < 100; sequence++ {
			status, err := runRequest(ctx, client, pod, sequence)
			stats.Record(status, err)
		}
		want := Stats{Requests: 100, Success: 97, NotFound: 2, Internal: 1}
		if stats != want {
			t.Fatalf("pod %d traffic: got %+v, want %+v", index, stats, want)
		}
		app.mu.Lock()
		for _, method := range []string{http.MethodPost, http.MethodGet, http.MethodPut} {
			if app.methods[method] == 0 {
				t.Errorf("pod %d has no %s requests", index, method)
			}
		}
		app.mu.Unlock()
	}
}

func TestRampIsGradualAndStaysAtTarget(t *testing.T) {
	for _, test := range []struct {
		elapsed time.Duration
		want    float64
	}{
		{-time.Second, 5},
		{0, 5},
		{90 * time.Second, 17.5},
		{180 * time.Second, 30},
		{time.Hour, 30},
	} {
		if got := plannedRPS(test.elapsed, 180*time.Second, 30); math.Abs(got-test.want) > 0.00001 {
			t.Errorf("elapsed %s: got %f, want %f", test.elapsed, got, test.want)
		}
	}
}

func TestRestoredPodGetsNewSeedAfterLosingItsStorage(t *testing.T) {
	app, server := newTestApp(t, 0)
	pod := &Pod{URL: server.URL, IDs: []uint64{100}}
	ctx := context.Background()
	client := server.Client()
	if status, err := runRequest(ctx, client, pod, 1); status != 404 || err != nil {
		t.Fatalf("stale ID: status=%d error=%v", status, err)
	}
	if len(pod.IDs) != 0 {
		t.Fatalf("stale IDs retained: %v", pod.IDs)
	}
	if status, err := runRequest(ctx, client, pod, 2); status != 201 || err != nil {
		t.Fatalf("new seed: status=%d error=%v", status, err)
	}
	app.mu.Lock()
	itemCount := len(app.items)
	app.mu.Unlock()
	if itemCount != 1 || len(pod.IDs) != 1 {
		t.Fatalf("new seed not stored: app item count=%d ids=%v", itemCount, pod.IDs)
	}
	if status, err := runRequest(ctx, client, pod, 3); status != 200 || err != nil {
		t.Fatalf("new seed PUT: status=%d error=%v", status, err)
	}
}

func TestInvalidCreateResponsesAreRejected(t *testing.T) {
	for _, body := range []string{`{`, `{}`, `{"id":0}`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusCreated)
				fmt.Fprint(w, body)
			}))
			defer server.Close()
			pod := &Pod{URL: server.URL}
			if _, err := createItem(context.Background(), server.Client(), pod, 0); err == nil {
				t.Fatal("malformed POST response was accepted")
			}
			if len(pod.IDs) != 0 {
				t.Fatalf("invalid ID added: %v", pod.IDs)
			}
		})
	}
}

func TestShutdownInterruptsSchedulingWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var posts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := posts.Add(1)
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"id":%d}`, count)
		if count == 2 {
			time.AfterFunc(20*time.Millisecond, cancel)
		}
	}))
	defer server.Close()
	start := time.Now()
	if err := run(ctx, server.Client(), []*Pod{{URL: server.URL}}, 1, time.Second); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed >= 500*time.Millisecond {
		t.Fatalf("shutdown waited for the next request slot: %s", elapsed)
	}
	if posts.Load() != 2 {
		t.Fatalf("requests continued after cancellation: %d", posts.Load())
	}
}

func TestBaseURLValidation(t *testing.T) {
	for _, raw := range []string{"", "localhost:8080", "/items", "ftp://example.com", "http://", "http://example.com?query=1", "http://example.com#fragment", "http://user:password@example.com"} {
		if validateURL(raw) == nil {
			t.Errorf("invalid base URL accepted: %q", raw)
		}
	}
	for _, raw := range []string{"http://app-1:8080", "https://example.com", "http://127.0.0.1:8080"} {
		if err := validateURL(raw); err != nil {
			t.Errorf("valid base URL %q: %v", raw, err)
		}
	}
}
