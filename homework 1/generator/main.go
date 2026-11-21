package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

type HandlerInfo struct {
	Method  string `json:"method"`
	Path    string `json:"path"`
	Handler string `json:"handler"`
}

func main() {
	appURL := getenv("APP_URL", "http://app-1:8080")
	grafURL := getenv("GRAFANA_URL", "http://grafana:3000")
	user := getenv("GRAFANA_ADMIN_USER", "admin")
	pass := os.Getenv("GRAFANA_ADMIN_PASSWORD")

	handlers := fetchHandlers(appURL)
	log.Printf("got %d handlers", len(handlers))

	dash := buildDashboard(handlers)
	if err := pushDashboard(grafURL, user, pass, dash); err != nil {
		log.Fatal(err)
	}
	log.Println("dashboard installed")
}

func fetchHandlers(base string) []HandlerInfo {

	var hs []HandlerInfo
		resp, err := http.Get(base + "/handlers")
		if err == nil && resp.StatusCode == 200 {
			json.NewDecoder(resp.Body).Decode(&hs)
			resp.Body.Close()
			return hs
		}
	log.Fatal("cannot fetch handlers")
	return nil
}

func buildDashboard(handlers []HandlerInfo) map[string]any {
	panels := []any{}
	id := 1
	for _, h := range handlers {
		// latency
		panels = append(panels, panelLatency(id, h)); id++
		// rps
		panels = append(panels, panelRPS(id, h)); id++
		// errors
		panels = append(panels, panelErrors(id, h)); id++
	}

	return map[string]any{
		"dashboard": map[string]any{
			"uid":       "generated-handlers",
			"title":     "Handlers monitoring",
			"editable":  false,
			"schemaVersion": 39,
			"refresh":   "5s",
			"time":      map[string]string{"from": "now-15m", "to": "now"},
			"templating": map[string]any{
				"list": []any{
					varQuery("dc", "label_values(http_requests_total, datacenter)"),
					varQuery("pod", "label_values(http_requests_total, pod)"),
				},
			},
			"panels": panels,
		},
		"overwrite": true,
	}
}

func varQuery(name, query string) map[string]any {
	return map[string]any{
		"name": name, "type": "query", "datasource": dsRef(),
		"query": query, "includeAll": true, "multi": true,
		"allValue": ".*",
		"refresh": 1,
	}
}

func dsRef() map[string]any {
	return map[string]any{"type": "prometheus", "uid": "prometheus-main"}
}

func panelLatency(id int, h HandlerInfo) map[string]any {
	sel := fmt.Sprintf(`handler="%s",method="%s",datacenter=~"$dc",pod=~"$pod"`, h.Handler, h.Method)
	targets := []any{}
	for _, q := range []float64{0.5, 0.95, 0.99} {
		targets = append(targets, map[string]any{
			"expr": fmt.Sprintf(
				`histogram_quantile(%v, sum(rate(http_request_duration_seconds_bucket{%s}[5m])) by (le))`,
				q, sel),
			"legendFormat": fmt.Sprintf("p%d", int(q*100)),
			"datasource": dsRef(),
		})
	}
	return panel(id, fmt.Sprintf("%s %s — Latency", h.Method, h.Path), targets)
}

func panelRPS(id int, h HandlerInfo) map[string]any {
	sel := fmt.Sprintf(`handler="%s",method="%s",datacenter=~"$dc",pod=~"$pod"`, h.Handler, h.Method)
	return panel(id, fmt.Sprintf("%s %s — RPS", h.Method, h.Path), []any{map[string]any{
		"expr": fmt.Sprintf(`sum(rate(http_requests_total{%s}[5m]))`, sel),
		"legendFormat": "rps", "datasource": dsRef(),
	}})
}

func panelErrors(id int, h HandlerInfo) map[string]any {
	sel := fmt.Sprintf(`handler="%s",method="%s",datacenter=~"$dc",pod=~"$pod",status=~"[45].."`, h.Handler, h.Method)
	return panel(id, fmt.Sprintf("%s %s — Errors", h.Method, h.Path), []any{map[string]any{
		"expr": fmt.Sprintf(`sum(rate(http_requests_total{%s}[5m])) by (status)`, sel),
		"legendFormat": "{{status}}", "datasource": dsRef(),
	}})
}

func panel(id int, title string, targets []any) map[string]any {
	return map[string]any{
		"id": id, "title": title, "type": "timeseries", "datasource": dsRef(),
		"gridPos": map[string]int{"h": 8, "w": 12},
		"targets": targets,
	}
}

func pushDashboard(base, user, pass string, dash map[string]any) error {
	body, _ := json.Marshal(dash)
	req, _ := http.NewRequest("POST", base+"/api/dashboards/db", bytes.NewReader(body))
	req.SetBasicAuth(user, pass)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("grafana %d: %s", resp.StatusCode, b)
	}
	return nil
}

func getenv(k, d string) string { if v := os.Getenv(k); v != "" { return v }; return d }