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

	"hw_task1/internal/handlers"
	"hw_task1/internal/storage"
	"hw_task1/pkg/monitoring"
)

type config struct {
	addr          string
	errorRate     float64
	medianLatency time.Duration
	podID         string
	dc            string
}

func loadConfig() (config, error) {
	cfg := config{
		addr:  envOr("ADDR", ":8080"),
		podID: envOr("POD_ID", "unknown"),
		dc:    envOr("DATACENTER", "unknown"),
	}
	if v := os.Getenv("ERROR_RATE"); v != "" {
		rate, err := strconv.ParseFloat(v, 64)
		if err != nil || rate < 0 || rate > 1 {
			return cfg, fmt.Errorf("ERROR_RATE must be a number in [0, 1], got %q", v)
		}
		cfg.errorRate = rate
	}
	if v := os.Getenv("LATENCY_MS"); v != "" {
		ms, err := strconv.ParseFloat(v, 64)
		if err != nil || ms < 0 {
			return cfg, fmt.Errorf("LATENCY_MS must be a non-negative number, got %q", v)
		}
		cfg.medianLatency = time.Duration(ms * float64(time.Millisecond))
	}
	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	healthcheck := flag.Bool("healthcheck", false, "probe GET /health of the local instance and exit")
	flag.Parse()

	cfg, err := loadConfig()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}

	if *healthcheck {
		os.Exit(probe(cfg.addr))
	}

	if err := run(cfg); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}

func run(cfg config) error {
	log := slog.With("pod", cfg.podID, "dc", cfg.dc)

	mux := http.NewServeMux()
	mon := monitoring.New(mux, monitoring.Config{Pod: cfg.podID, DC: cfg.dc})
	handlers.NewItems(storage.New(), cfg.errorRate, cfg.medianLatency).Register(mon)

	mux.HandleFunc("GET /health", handlers.Health)
	mux.Handle("GET /metrics", mon.MetricsHandler())
	mux.Handle("GET /handlers", mon.HandlersHandler())

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.addr, "error_rate", cfg.errorRate, "median_latency", cfg.medianLatency)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func probe(addr string) int {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + net.JoinHostPort("localhost", port) + "/health")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "health status:", resp.Status)
		return 1
	}
	return 0
}
