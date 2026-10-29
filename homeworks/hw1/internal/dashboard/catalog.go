package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"unicode/utf8"

	"highload/lab1/pkg/monitoring"
)

const maxCatalogBytes = 64 << 10

// Endpoint is one business route from GET /handlers.
// The generator does not keep its own copy of the application routes.
type Endpoint struct {
	Method string
	Path   string
}

// FetchCatalog reads GET /handlers. That request is service discovery:
// the route is not a business operation and is not part of the returned catalog.
func FetchCatalog(ctx context.Context, httpClient *http.Client, cfg Config) ([]Endpoint, error) {
	target, err := endpointURL(cfg.AppBaseURL, "/handlers")
	if err != nil {
		return nil, err
	}
	status, body, err := do(ctx, httpClient, cfg, "", "", http.MethodGet, target, nil, maxCatalogBytes)
	if err != nil {
		return nil, fmt.Errorf("fetch handlers: %w", err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("fetch handlers: %s", statusMessage(http.MethodGet, "/handlers", status, body, ""))
	}
	handlers, err := ParseCatalog(body)
	if err != nil {
		return nil, fmt.Errorf("fetch handlers: %w", err)
	}
	return handlers, nil
}

// ParseCatalog checks the JSON shape and rejects values that cannot be safely
// placed into a PromQL string or a Grafana variable expression.
func ParseCatalog(body []byte) ([]Endpoint, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil, fmt.Errorf("handlers response is empty")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var raw map[string]json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("handlers response is not a JSON object: %w", err)
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("handlers response must contain one JSON object")
	}
	if len(raw) != 1 {
		return nil, fmt.Errorf("handlers response must contain only the handlers field")
	}
	encoded, ok := raw["handlers"]
	if !ok || bytes.Equal(bytes.TrimSpace(encoded), []byte("null")) {
		return nil, fmt.Errorf("handlers field is required")
	}

	listDecoder := json.NewDecoder(bytes.NewReader(encoded))
	listDecoder.DisallowUnknownFields()
	var items []json.RawMessage
	if err := listDecoder.Decode(&items); err != nil {
		return nil, fmt.Errorf("handlers must be an array: %w", err)
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("handlers catalog is empty")
	}
	if len(items) > 64 {
		return nil, fmt.Errorf("handlers catalog has %d routes, the limit is 64", len(items))
	}

	handlers := make([]Endpoint, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		handler, err := parseEndpoint(item)
		if err != nil {
			return nil, err
		}
		key := handler.Method + " " + handler.Path
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("duplicate handler %s", key)
		}
		seen[key] = struct{}{}
		handlers = append(handlers, handler)
	}
	return handlers, nil
}

func parseEndpoint(raw json.RawMessage) (Endpoint, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var item struct {
		Method string `json:"method"`
		Path   string `json:"path"`
	}
	if err := decoder.Decode(&item); err != nil {
		return Endpoint{}, fmt.Errorf("handler entry is invalid: %w", err)
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return Endpoint{}, fmt.Errorf("handler entry must contain one JSON object")
	}
	handler := Endpoint{Method: item.Method, Path: item.Path}
	if err := handler.validate(); err != nil {
		return Endpoint{}, err
	}
	return handler, nil
}

func (e Endpoint) validate() error {
	if !monitoring.ValidMethod(e.Method) {
		return fmt.Errorf("handler method %q is invalid", e.Method)
	}
	if err := validatePath(e.Path); err != nil {
		return fmt.Errorf("handler path: %w", err)
	}
	return nil
}

func validatePath(path string) error {
	if len(path) > 256 {
		return fmt.Errorf("path is longer than 256 bytes")
	}
	if !utf8.ValidString(path) || !monitoring.ValidRoutePath(path) {
		return fmt.Errorf("path %q is invalid", path)
	}
	return nil
}
