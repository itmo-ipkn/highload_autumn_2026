// dashgen строит дашборд Grafana по ручкам, которые отдают приложения.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/DenisBorzenkov/ddia-2026/hw-1/internal/dashboard"
)

func main() {
	if err := run(context.Background()); err != nil {
		slog.Error("dashgen", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	apps := strings.Split(getenv("APP_URLS", "http://localhost:8080"), ",")
	g := &dashboard.Grafana{
		Client:   dashboard.Client{HTTP: &http.Client{Timeout: 5 * time.Second}},
		URL:      getenv("GRAFANA_URL", "http://localhost:3000"),
		User:     os.Getenv("GRAFANA_ADMIN_USER"),
		Password: os.Getenv("GRAFANA_ADMIN_PASSWORD"),
	}
	if g.User == "" || g.Password == "" {
		return errors.New("GRAFANA_ADMIN_USER and GRAFANA_ADMIN_PASSWORD are required")
	}
	opts := dashboard.Options{
		UID:           getenv("DASHBOARD_UID", "hw1-handlers"),
		Title:         getenv("DASHBOARD_TITLE", "HW-1: ручки сервиса items"),
		DatasourceUID: getenv("DATASOURCE_UID", "prometheus"),
	}

	handlers, err := collectHandlers(ctx, &g.Client, apps)
	if err != nil {
		return err
	}
	slog.Info("handlers", "count", len(handlers))

	if err := dashboard.Retry(ctx, time.Minute, 2*time.Second, g.Health); err != nil {
		return err
	}
	if err := g.Upsert(ctx, dashboard.Build(opts, handlers)); err != nil {
		return err
	}
	slog.Info("dashboard uploaded", "uid", opts.UID, "url", g.URL+"/d/"+opts.UID)

	if err := g.SetHome(ctx, opts.UID); err != nil {
		return err
	}
	// под этим пользователем дашборд только на просмотр: у Viewer нет прав на правку
	if login := os.Getenv("GRAFANA_VIEWER_USER"); login != "" {
		if err := g.EnsureViewer(ctx, login, os.Getenv("GRAFANA_VIEWER_PASSWORD")); err != nil {
			return err
		}
		slog.Info("viewer ready", "login", login)
	}
	return nil
}

// collectHandlers объединяет ручки всех экземпляров: дашборд общий.
func collectHandlers(ctx context.Context, c *dashboard.Client, apps []string) ([]dashboard.Handler, error) {
	seen := map[dashboard.Handler]bool{}
	for _, app := range apps {
		var hs []dashboard.Handler
		err := dashboard.Retry(ctx, time.Minute, 2*time.Second, func(ctx context.Context) error {
			var err error
			hs, err = c.FetchHandlers(ctx, strings.TrimSpace(app))
			return err
		})
		if err != nil {
			return nil, err
		}
		for _, h := range hs {
			seen[h] = true
		}
	}
	out := make([]dashboard.Handler, 0, len(seen))
	for h := range seen {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Route != out[j].Route {
			return out[i].Route < out[j].Route
		}
		return methodOrder(out[i].Method) < methodOrder(out[j].Method)
	})
	return out, nil
}

func methodOrder(m string) int {
	for i, v := range []string{"GET", "POST", "PUT", "PATCH", "DELETE"} {
		if v == m {
			return i
		}
	}
	return 99
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
