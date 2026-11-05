package app

import (
	"net/http"
	"time"
)

type handlerInfo struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

var businessHandlers = []handlerInfo{
	{Method: http.MethodPost, Path: "/items"},
	{Method: http.MethodGet, Path: "/items/{id}"},
	{Method: http.MethodPut, Path: "/items/{id}"},
}

type Config struct {
	Port              string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

type Application struct {
	middlewares *middlewares
	server      *http.Server
	storage     serverStorage
	config      Config
}

func NewApplication(config Config, metrics httpServerMetrics, storage serverStorage) *Application {
	app := &Application{
		middlewares: NewMiddlewares(metrics),
		storage:     storage,
		config:      config,
	}
	return app
}

func (a *Application) ListenAndServe() error {
	a.server = &http.Server{
		Addr:              ":" + a.config.Port,
		Handler:           a.routes(),
		ReadHeaderTimeout: a.config.ReadHeaderTimeout,
		ReadTimeout:       a.config.ReadTimeout,
		WriteTimeout:      a.config.WriteTimeout,
		IdleTimeout:       a.config.IdleTimeout,
	}
	return a.server.ListenAndServe()
}

func (a *Application) routes() http.Handler {
	mux := http.NewServeMux()

	business := []struct {
		pattern string
		method  string
		path    string
		handler http.Handler
	}{
		{"POST /items", http.MethodPost, "/items", http.HandlerFunc(a.createItem)},
		{"GET /items/{id}", http.MethodGet, "/items/{id}", http.HandlerFunc(a.getItem)},
		{"PUT /items/{id}", http.MethodPut, "/items/{id}", http.HandlerFunc(a.updateItem)},
	}
	for _, b := range business {
		mux.Handle(b.pattern, a.middlewares.Middleware(b.method, b.path, b.handler))
	}

	mux.HandleFunc("GET /health", a.health)
	mux.HandleFunc("GET /handlers", a.handlers)
	mux.Handle("GET /metrics", a.middlewares.metrics.HandleRegistry())

	return mux
}
