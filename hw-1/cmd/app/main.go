package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/DenisBorzenkov/ddia-2026/hw-1/internal/server"
)

func main() {
	addr := getenv("LISTEN_ADDR", ":8080")

	healthcheck := flag.Bool("healthcheck", false, "check /health and exit")
	flag.Parse()
	if *healthcheck {
		os.Exit(checkHealth(addr))
	}

	cfg := server.Config{
		PodID:      getenv("POD_ID", "local"),
		Datacenter: getenv("DATACENTER", "local"),
	}
	var err error
	if cfg.FailRate, err = strconv.ParseFloat(getenv("FAIL_RATE", "0"), 64); err != nil {
		slog.Error("FAIL_RATE", "err", err)
		os.Exit(1)
	}
	if cfg.MaxDelay, err = time.ParseDuration(getenv("MAX_DELAY", "0s")); err != nil {
		slog.Error("MAX_DELAY", "err", err)
		os.Exit(1)
	}

	r, err := server.New(cfg)
	if err != nil {
		slog.Error("router", "err", err)
		os.Exit(1)
	}

	srv := &http.Server{Addr: addr, Handler: r, ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("listening", "addr", addr, "pod", cfg.PodID, "datacenter", cfg.Datacenter)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown", "err", err)
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// checkHealth - для HEALTHCHECK в образе без shell и curl.
func checkHealth(addr string) int {
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://localhost" + addr + "/health")
	if err != nil {
		return 1
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
