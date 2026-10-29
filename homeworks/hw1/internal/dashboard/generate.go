package dashboard

import (
	"context"
	"errors"
	"net/http"
)

// Result is the dashboard Grafana stored.
type Result struct {
	UID    string
	Panels int
}

// Generate loads the handler catalog and creates or updates the dashboard.
func Generate(ctx context.Context, httpClient *http.Client, cfg Config) (Result, error) {
	if err := cfg.validate(); err != nil {
		return Result{}, err
	}
	if httpClient == nil {
		return Result{}, errors.New("http client is required")
	}
	handlers, err := FetchCatalog(ctx, httpClient, cfg)
	if err != nil {
		return Result{}, redactError(cfg.Password, err)
	}
	spec, err := Build(handlers)
	if err != nil {
		return Result{}, err
	}
	if err := Apply(ctx, httpClient, cfg, spec); err != nil {
		return Result{}, redactError(cfg.Password, err)
	}
	return Result{UID: deref(spec.Uid), Panels: len(spec.Panels)}, nil
}
