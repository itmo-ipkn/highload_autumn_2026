package load

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"highload-hw1/internal/api"
	"highload-hw1/internal/items"
)

func TestRamp(t *testing.T) {
	for _, tc := range []struct {
		elapsed time.Duration
		want    float64
	}{{0, 1}, {75 * time.Second, 15.5}, {150 * time.Second, 30}, {300 * time.Second, 30}} {
		if got := Rate(tc.elapsed, 150*time.Second, 30); got != tc.want {
			t.Fatalf("rate = %v, want %v", got, tc.want)
		}
	}
}

func TestTrafficAgainstIndependentStores(t *testing.T) {
	counts := map[int]int{}
	methods := map[string]int{}
	for _, pod := range []string{"first", "second"} {
		handler := api.New(items.NewStore(), pod, "test")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, r)
			counts[recorder.Code]++
			methods[r.Method]++
			for k, values := range recorder.Header() {
				w.Header()[k] = values
			}
			w.WriteHeader(recorder.Code)
			_, _ = w.Write(recorder.Body.Bytes())
		}))
		target := Target{URL: server.URL}
		for step := 0; step < 300; step++ {
			if err := target.Request(context.Background(), server.Client(), step); err != nil {
				t.Fatal(err)
			}
		}
		server.Close()
	}
	if counts[400] != 12 || counts[404] != 12 || counts[200]+counts[201] != 576 {
		t.Fatalf("codes: %v", counts)
	}
	for _, method := range []string{"GET", "POST", "PUT"} {
		if methods[method] == 0 {
			t.Fatalf("нет %s", method)
		}
	}
}

func TestCancelStopsWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		Run(ctx, &http.Client{}, []Target{{URL: "http://unused"}}, time.Minute, 30, func(error) { t.Error("unexpected request") })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("отмена не остановила нагрузку")
	}
}

func TestUnexpectedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	target := Target{URL: server.URL}
	if err := target.Request(context.Background(), server.Client(), 0); err == nil {
		t.Fatal("ошибка ответа потеряна")
	}
}
