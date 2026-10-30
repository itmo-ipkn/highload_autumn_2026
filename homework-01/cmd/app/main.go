package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Fadet/highload-hw1/internal/api"
	"github.com/Fadet/highload-hw1/internal/metrics"
	"github.com/Fadet/highload-hw1/internal/store"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "probe GET /health of a running instance and exit")
	flag.Parse()

	addr := getenv("ADDR", ":8080")

	if *healthcheck {
		if err := probeHealth(addr); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if err := run(addr); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(addr string) error {
	podID := getenv("POD_ID", "local")
	datacenter := getenv("DATACENTER", "local")

	opts, err := loadOptions()
	if err != nil {
		return err
	}

	registry := prometheus.NewRegistry()
	reg := prometheus.WrapRegistererWith(prometheus.Labels{
		"pod":        podID,
		"datacenter": datacenter,
	}, registry)
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	mux := http.NewServeMux()

	router := metrics.NewRouter(reg, mux)
	api.New(store.New(), opts).Register(router)

	mux.HandleFunc("GET /health", api.Health)
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.Handle("GET /handlers", router.HandlersHandler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		slog.Info("starting server",
			"addr", addr, "pod", podID, "datacenter", datacenter,
			"failure_rate", opts.FailureRate, "latency_mean", opts.LatencyMean)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	slog.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func loadOptions() (api.Options, error) {
	var opts api.Options

	if v := os.Getenv("FAILURE_RATE"); v != "" {
		rate, err := strconv.ParseFloat(v, 64)
		if err != nil || rate < 0 || rate > 1 {
			return opts, fmt.Errorf("FAILURE_RATE must be a number in [0, 1], got %q", v)
		}
		opts.FailureRate = rate
	}

	if v := os.Getenv("LATENCY_MEAN"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return opts, fmt.Errorf("LATENCY_MEAN must be a non-negative duration like 20ms, got %q", v)
		}
		opts.LatencyMean = d
	}

	return opts, nil
}

func probeHealth(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("parse ADDR %q: %w", addr, err)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health status %d", resp.StatusCode)
	}
	return nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
