package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type handlerInfo struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

const datasourceUID = "prometheus"

func main() {
	if err := generate(); err != nil {
		fmt.Fprintln(os.Stderr, "dashboard-generator:", err)
		os.Exit(1)
	}
}

func generate() error {
	itemsURL := env("ITEMS_URL", "http://items-1:8080")
	grafanaURL := strings.TrimRight(env("GRAFANA_URL", "http://grafana:3000"), "/")
	username := os.Getenv("GRAFANA_ADMIN_USER")
	password := os.Getenv("GRAFANA_ADMIN_PASSWORD")
	if username == "" || password == "" {
		return errors.New("GRAFANA_ADMIN_USER and GRAFANA_ADMIN_PASSWORD are required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	request, err := http.NewRequest(http.MethodGet, strings.TrimRight(itemsURL, "/")+"/handlers", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("fetch handlers: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("fetch handlers returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	var handlers []handlerInfo
	if err := json.NewDecoder(response.Body).Decode(&handlers); err != nil {
		return fmt.Errorf("decode handlers: %w", err)
	}
	if len(handlers) == 0 {
		return errors.New("application returned no instrumented handlers")
	}

	dashboard := buildDashboard(handlers)
	payload, err := json.Marshal(map[string]any{
		"dashboard": dashboard,
		"folderId":  0,
		"overwrite": true,
	})
	if err != nil {
		return fmt.Errorf("encode dashboard: %w", err)
	}
	request, err = http.NewRequest(http.MethodPost, grafanaURL+"/api/dashboards/db", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.SetBasicAuth(username, password)
	response, err = client.Do(request)
	if err != nil {
		return fmt.Errorf("publish dashboard: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("publish dashboard returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	_, _ = io.Copy(io.Discard, response.Body)
	fmt.Printf("Generated Grafana dashboard with %d handlers.\n", len(handlers))
	return nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func buildDashboard(handlers []handlerInfo) map[string]any {
	panels := make([]map[string]any, 0, 4+len(handlers)*3)
	filters := `datacenter=~"$datacenter",pod_id=~"$pod_id"`
	panels = append(panels,
		statPanel(1, "Requests / second", 0, 0, "reqps", fmt.Sprintf(`sum(rate(http_server_requests_total{%s}[$__rate_interval]))`, filters)),
		statPanel(2, "p95 latency", 8, 0, "s", fmt.Sprintf(`histogram_quantile(0.95, sum by (le) (rate(http_server_request_duration_seconds_bucket{%s}[$__rate_interval])))`, filters)),
		statPanel(3, "4xx + 5xx responses", 16, 0, "short", fmt.Sprintf(`sum(increase(http_server_requests_total{%s,status_code=~"4..|5.."}[$__range])) or vector(0)`, filters)),
		errorTablePanel(4, 0, 5, filters),
	)
	for handlerIndex, handler := range handlers {
		handlerID := handler.Method + " " + handler.Path
		selector := handlerSelector(handlerID)
		groupY := 13 + handlerIndex*8
		firstPanelID := handlerIndex*3 + 5
		panels = append(panels,
			panel(firstPanelID, "Latency — "+handlerID, 0, groupY, "s", latencyTargets(selector)),
			panel(firstPanelID+1, "Requests / second — "+handlerID, 8, groupY, "reqps", []map[string]any{
				queryTarget("A", requestRateQuery(selector), "requests/sec"),
			}),
			errorPanel(firstPanelID+2, "Errors — "+handlerID, 16, groupY, selector),
		)
	}

	return map[string]any{
		"uid":           "items-golden-signals",
		"title":         "Items Service Golden Signals",
		"schemaVersion": 39,
		"version":       1,
		"editable":      false,
		"refresh":       "30s",
		"time":          map[string]any{"from": "now-15m", "to": "now"},
		"templating": map[string]any{"list": []map[string]any{
			variable("datacenter", "Datacenter", "datacenter"),
			variable("pod_id", "Pod", "pod_id"),
		}},
		"panels": panels,
	}
}

func variable(name, label, metricLabel string) map[string]any {
	query := fmt.Sprintf("label_values(http_server_requests_total, %s)", metricLabel)
	return map[string]any{
		"name":       name,
		"label":      label,
		"type":       "query",
		"datasource": prometheusDatasource(),
		"definition": query,
		"query":      map[string]any{"query": query, "refId": "StandardVariableQuery"},
		"multi":      true,
		"includeAll": true,
		"allValue":   ".*",
		"refresh":    1,
		"current":    map[string]any{"selected": true, "text": "All", "value": "$__all"},
		"sort":       1,
	}
}

func panel(id int, title string, x, y int, unit string, targets []map[string]any) map[string]any {
	return map[string]any{
		"id":          id,
		"title":       title,
		"type":        "timeseries",
		"datasource":  prometheusDatasource(),
		"gridPos":     map[string]int{"h": 8, "w": 8, "x": x, "y": y},
		"targets":     targets,
		"fieldConfig": map[string]any{"defaults": map[string]any{"unit": unit, "color": map[string]any{"mode": "palette-classic"}}, "overrides": []any{}},
		"options":     map[string]any{"legend": map[string]any{"displayMode": "list", "placement": "bottom"}, "tooltip": map[string]any{"mode": "multi"}},
	}
}

func statPanel(id int, title string, x, y int, unit, query string) map[string]any {
	target := queryTarget("A", query, title)
	target["instant"] = true
	return map[string]any{
		"id": id, "title": title, "type": "stat", "datasource": prometheusDatasource(),
		"gridPos":     map[string]int{"h": 5, "w": 8, "x": x, "y": y},
		"targets":     []map[string]any{target},
		"fieldConfig": map[string]any{"defaults": map[string]any{"unit": unit, "color": map[string]any{"mode": "palette-classic"}}, "overrides": []any{}},
		"options":     map[string]any{"reduceOptions": map[string]any{"calcs": []string{"lastNotNull"}, "fields": "", "values": false}, "orientation": "auto", "textMode": "auto", "colorMode": "value", "graphMode": "area", "justifyMode": "auto"},
	}
}

func errorTablePanel(id, x, y int, filters string) map[string]any {
	query := fmt.Sprintf(`sum by (handler, status_code) (increase(http_server_requests_total{%s,status_code=~"4..|5.."}[$__range]))`, filters)
	return map[string]any{
		"id": id, "title": "Errors by route and status code", "type": "table", "datasource": prometheusDatasource(),
		"description": "Число ответов 4xx и 5xx за выбранный интервал. Пустая таблица означает, что ошибок не было.",
		"gridPos":     map[string]int{"h": 8, "w": 24, "x": x, "y": y},
		"targets":     []map[string]any{{"refId": "A", "expr": query, "format": "table", "instant": true}},
		"fieldConfig": map[string]any{"defaults": map[string]any{"unit": "short"}, "overrides": []any{}},
		"options":     map[string]any{"showHeader": true, "cellHeight": "sm"},
	}
}

func errorPanel(id int, title string, x, y int, selector string) map[string]any {
	query := fmt.Sprintf(`sum by (status_code) (increase(http_server_requests_total{%s,status_code=~"4..|5.."}[$__rate_interval]))`, selector)
	result := panel(id, title, x, y, "short", []map[string]any{
		queryTarget("A", query, "{{status_code}}"),
	})
	result["description"] = "Количество ответов 4xx и 5xx по коду. Если ошибок не было, график будет пустым."
	return result
}

func prometheusDatasource() map[string]string {
	return map[string]string{"type": "prometheus", "uid": datasourceUID}
}

func handlerSelector(handlerID string) string {
	return fmt.Sprintf(`handler=%s,datacenter=~"$datacenter",pod_id=~"$pod_id"`, strconv.Quote(handlerID))
}

func latencyTargets(selector string) []map[string]any {
	quantiles := []struct {
		value float64
		name  string
	}{{0.50, "p50"}, {0.95, "p95"}, {0.99, "p99"}}
	targets := make([]map[string]any, 0, len(quantiles))
	for index, quantile := range quantiles {
		query := fmt.Sprintf(`histogram_quantile(%0.2f, sum by (le) (rate(http_server_request_duration_seconds_bucket{%s}[$__rate_interval])))`, quantile.value, selector)
		targets = append(targets, queryTarget(string(rune('A'+index)), query, quantile.name))
	}
	return targets
}

func requestRateQuery(selector string) string {
	return fmt.Sprintf(`sum(rate(http_server_requests_total{%s}[$__rate_interval]))`, selector)
}

func queryTarget(refID, query, legend string) map[string]any {
	return map[string]any{"refId": refID, "expr": query, "legendFormat": legend}
}
