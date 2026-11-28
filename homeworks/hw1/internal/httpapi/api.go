package httpapi

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"runtime/debug"

	"highload/lab1/internal/item"
	"highload/lab1/pkg/monitoring"
)

type repository interface {
	Create(name, value string) (item.Item, error)
	Get(id string) (item.Item, error)
	Update(id, name, value string) (item.Item, error)
}

type API struct {
	store        repository
	health       *Health
	maxBodyBytes int64
	handler      http.Handler
}

func New(store repository, health *Health, maxBodyBytes int64, metrics *monitoring.Metrics) (*API, error) {
	if isNil(store) {
		return nil, errors.New("store is required")
	}
	if health == nil {
		return nil, errors.New("health is required")
	}
	if maxBodyBytes <= 0 {
		return nil, errors.New("max body bytes must be positive")
	}
	if metrics == nil {
		return nil, errors.New("metrics are required")
	}

	api := &API{store: store, health: health, maxBodyBytes: maxBodyBytes}
	mux := http.NewServeMux()
	routes, err := monitoring.NewRegistrar(metrics, recoverHTTP)
	if err != nil {
		return nil, err
	}
	business := monitoring.Options{Instrument: true, Catalog: true}
	registered := []struct {
		method  string
		path    string
		handler http.Handler
		opts    monitoring.Options
	}{
		{method: http.MethodPost, path: "/items", handler: http.HandlerFunc(api.create), opts: business},
		{method: http.MethodGet, path: "/items/{id}", handler: http.HandlerFunc(api.get), opts: business},
		{method: http.MethodPut, path: "/items/{id}", handler: http.HandlerFunc(api.update), opts: business},
		{method: http.MethodGet, path: "/handlers", handler: routes, opts: monitoring.Options{Instrument: true, Catalog: false}},
	}
	for _, route := range registered {
		if err := routes.Handle(mux, route.method, route.path, route.handler, route.opts); err != nil {
			return nil, fmt.Errorf("register %s %s: %w", route.method, route.path, err)
		}
	}
	// /health and /metrics are not business operations and are not instrumented.
	mux.Handle("GET /health", health)
	mux.Handle("GET /metrics", metrics.Handler())
	api.handler = withRecovery(mux)
	return api, nil
}

func (a *API) Handler() http.Handler {
	return a.handler
}

// withRecovery covers /health and /metrics. Those routes are registered on the mux
// directly, so the metrics wrapper never sees them. Instrumented routes recover
// inside that wrapper first; this layer then sees a normal response.
func withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := monitoring.NewStatusRecorder(w)
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}
			recoverHTTP(recorder, r, recovered, recorder.WroteHeader())
		}()
		next.ServeHTTP(recorder, r)
	})
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func recoverHTTP(w http.ResponseWriter, _ *http.Request, recovered any, wroteHeader bool) {
	log.Printf("http panic: %v\n%s", recovered, debug.Stack())
	if wroteHeader {
		return
	}
	writeError(w, http.StatusInternalServerError, "internal error")
}
