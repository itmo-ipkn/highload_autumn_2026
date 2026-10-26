// Package server собирает роутер: бизнес-ручки с метриками и служебные без них.
package server

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/DenisBorzenkov/ddia-2026/hw-1/internal/api"
	"github.com/DenisBorzenkov/ddia-2026/hw-1/internal/chaos"
	"github.com/DenisBorzenkov/ddia-2026/hw-1/internal/metrics"
	"github.com/DenisBorzenkov/ddia-2026/hw-1/internal/store"
)

type Config struct {
	PodID      string
	Datacenter string
	FailRate   float64
	MaxDelay   time.Duration
}

func New(cfg Config) (http.Handler, error) {
	m := metrics.New(cfg.PodID, cfg.Datacenter)

	biz := chi.NewRouter()
	biz.Use(m.Middleware)
	// chaos в группе: срабатывает после роутинга, и 500 попадают в метрики с шаблоном ручки
	biz.Group(func(g chi.Router) {
		g.Use(chaos.Middleware(cfg.FailRate, cfg.MaxDelay))
		api.New(store.New()).Routes(g)
	})

	handlers, err := metrics.HandlersHandler(biz)
	if err != nil {
		return nil, err
	}

	r := chi.NewRouter()
	r.Get("/health", api.Health)
	r.Method(http.MethodGet, "/metrics", m.Handler())
	r.Method(http.MethodGet, "/handlers", handlers)
	r.Mount("/", biz)
	return r, nil
}
