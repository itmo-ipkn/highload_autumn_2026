package httpapi

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"highload/lab1/internal/item"
)

func TestServeClearsReadinessOnShutdown(t *testing.T) {
	t.Parallel()

	store := item.NewStore()
	health := NewHealth()
	api, err := New(store, health, 1024, testMetrics(t))
	if err != nil {
		t.Fatalf("new api: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := &http.Server{Handler: api.Handler()}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- Serve(ctx, server, listener, health, time.Second)
	}()

	endpoint := "http://" + listener.Addr().String() + "/health"
	waitUntil(t, time.Second, func() bool {
		status, err := getStatus(endpoint)
		return err == nil && status == http.StatusOK && health.Ready()
	})

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown timed out")
	}
	if health.Ready() {
		t.Fatal("readiness stayed set after shutdown")
	}
	if _, err := getStatus(endpoint); err == nil {
		t.Fatal("health request succeeded after shutdown")
	}
}

func getStatus(endpoint string) (int, error) {
	resp, err := http.Get(endpoint)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return resp.StatusCode, nil
}

func waitUntil(t *testing.T, limit time.Duration, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before the deadline")
}
