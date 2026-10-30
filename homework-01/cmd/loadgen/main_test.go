package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Fadet/highload-hw1/internal/api"
	"github.com/Fadet/highload-hw1/internal/store"
)

func TestRateAt(t *testing.T) {
	ramp := 100 * time.Second
	tests := []struct {
		elapsed time.Duration
		want    float64
	}{
		{0, minRPS},
		{time.Second, minRPS},
		{50 * time.Second, 15},
		{100 * time.Second, 30},
		{time.Hour, 30},
	}
	for _, tt := range tests {
		if got := rateAt(tt.elapsed, ramp, 30); got != tt.want {
			t.Errorf("rateAt(%v) = %v, want %v", tt.elapsed, got, tt.want)
		}
	}
	if got := rateAt(0, 0, 30); got != 30 {
		t.Errorf("no ramp: got %v", got)
	}
}

func TestPickRequest(t *testing.T) {
	if r := pickRequest(0.9, 0.9, 0, 0.05); r.kind != kindCreate || r.method != http.MethodPost {
		t.Fatalf("with no known ids expected POST, got %+v", r)
	}

	tests := []struct {
		kindRoll float64
		method   string
	}{
		{0.1, http.MethodPost},
		{0.5, http.MethodGet},
		{0.9, http.MethodPut},
	}
	for _, tt := range tests {
		r := pickRequest(tt.kindRoll, 0.9, 10, 0.05)
		if r.method != tt.method {
			t.Errorf("kindRoll %v: method %s, want %s", tt.kindRoll, r.method, tt.method)
		}
		if r.kind != kindCreate && (r.id < 1 || r.id > 10) {
			t.Errorf("existing id out of range: %d", r.id)
		}
	}

	if r := pickRequest(0.5, 0.01, 10, 0.05); r.id < missingBase {
		t.Errorf("expected missing id, got %d", r.id)
	}
}

func TestRunHitsAllRoutesOnAllTargets(t *testing.T) {
	type key struct{ target, route string }
	var mu sync.Mutex
	hits := map[key]int{}

	newTarget := func(name string) *httptest.Server {
		mux := http.NewServeMux()
		api.New(store.New(), api.Options{}).Register(recordingRouter{mux: mux, record: func(route string) {
			mu.Lock()
			hits[key{name, route}]++
			mu.Unlock()
		}})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		return srv
	}
	a, b := newTarget("a"), newTarget("b")

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	st := run(ctx, config{
		targets:       []string{a.URL, b.URL},
		targetRPS:     400,
		notFoundRatio: 0.2,
		statsEvery:    time.Hour,
	}, http.DefaultClient)

	for _, name := range []string{"a", "b"} {
		for _, route := range []string{"POST /items", "GET /items/{id}", "PUT /items/{id}"} {
			if hits[key{name, route}] == 0 {
				t.Errorf("target %s: no requests to %s", name, route)
			}
		}
	}

	_, codes, _ := st.snapshot()
	for _, code := range []string{"200", "201", "404"} {
		if codes[code] == 0 {
			t.Errorf("no %s responses, codes: %v", code, codes)
		}
	}
	if total := codes["200"] + codes["201"] + codes["404"]; total < 300 {
		t.Errorf("too few responses: %v", codes)
	}
}

type recordingRouter struct {
	mux    *http.ServeMux
	record func(route string)
}

func (r recordingRouter) Handle(pattern string, h http.Handler) {
	r.mux.Handle(pattern, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.record(pattern)
		h.ServeHTTP(w, req)
	}))
}
