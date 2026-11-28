package monitoring

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
)

// Endpoint is one business route exposed by GET /handlers.
type Endpoint struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

// Options separates metrics collection from the business catalog.
// /handlers is instrumented and omitted from the catalog.
// /health and /metrics stay outside this registrar.
type Options struct {
	Instrument bool
	Catalog    bool
}

type Registrar struct {
	metrics *Metrics
	onPanic PanicHandler
	mu      sync.Mutex
	catalog []Endpoint
	seen    map[string]struct{}
}

func NewRegistrar(metrics *Metrics, onPanic PanicHandler) (*Registrar, error) {
	if metrics == nil {
		return nil, errors.New("metrics are required")
	}
	if onPanic == nil {
		return nil, errors.New("panic handler is required")
	}
	return &Registrar{
		metrics: metrics,
		onPanic: onPanic,
		catalog: []Endpoint{},
		seen:    map[string]struct{}{},
	}, nil
}

// Handle registers method and path on mux. The route label is the path template
// passed here, never the request URL.
func (reg *Registrar) Handle(mux *http.ServeMux, method, path string, handler http.Handler, opts Options) error {
	if mux == nil {
		return errors.New("mux is required")
	}
	if handler == nil {
		return errors.New("handler is required")
	}
	if !ValidMethod(method) {
		return fmt.Errorf("method %q is invalid", method)
	}
	if !ValidRoutePath(path) {
		return fmt.Errorf("path %q is invalid", path)
	}

	pattern := method + " " + path
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if _, ok := reg.seen[pattern]; ok {
		return fmt.Errorf("route %s is already registered", pattern)
	}
	reg.seen[pattern] = struct{}{}
	if opts.Catalog {
		reg.catalog = append(reg.catalog, Endpoint{Method: method, Path: path})
	}

	if opts.Instrument {
		handler = reg.metrics.wrap(method, path, handler, reg.onPanic)
	}
	mux.Handle(pattern, handler)
	return nil
}

// Catalog returns a copy of the business routes in registration order.
func (reg *Registrar) Catalog() []Endpoint {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	out := make([]Endpoint, len(reg.catalog))
	copy(out, reg.catalog)
	return out
}

func (reg *Registrar) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	body := struct {
		Handlers []Endpoint `json:"handlers"`
	}{Handlers: reg.Catalog()}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(body)
}
