package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"highload-hw1/internal/dashboard"
)

func main() {
	if err := run(); err != nil {
		slog.Error("Дашборд не создан", "error", err)
		os.Exit(1)
	}
}

func run() error {
	keys := []string{"APP_URLS", "GRAFANA_URL", "GRAFANA_ADMIN_USER", "GRAFANA_ADMIN_PASSWORD"}
	for _, key := range keys {
		if os.Getenv(key) == "" {
			return fmt.Errorf("не задана переменная %s", key)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	httpClient := &http.Client{Timeout: 10 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	endpoints, err := dashboard.Discover(ctx, httpClient, strings.Split(os.Getenv("APP_URLS"), ","))
	if err != nil {
		return err
	}
	client := dashboard.Client{HTTP: httpClient,
		Username: os.Getenv("GRAFANA_ADMIN_USER"), Password: os.Getenv("GRAFANA_ADMIN_PASSWORD")}
	if err := client.Publish(ctx, os.Getenv("GRAFANA_URL"), dashboard.Build(endpoints)); err != nil {
		return err
	}
	slog.Info("Дашборд сохранён", "uid", dashboard.UID, "ручек", len(endpoints), "панелей", 3*len(endpoints))
	return nil
}
