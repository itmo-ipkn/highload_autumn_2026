package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type HandlerInfo struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

type HandlersResponse struct {
	Handlers []HandlerInfo `json:"handlers"`
}

type DashboardPanel struct {
	ID         int                 `json:"id"`
	Title      string              `json:"title"`
	Type       string              `json:"type"`
	GridPos    GridPosition        `json:"gridPos"`
	Targets    []PanelTarget       `json:"targets"`
	Datasource DashboardDatasource `json:"datasource"`
}

type GridPosition struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

type PanelTarget struct {
	Expr         string `json:"expr"`
	RefID        string `json:"refId"`
	LegendFormat string `json:"legendFormat,omitempty"`
}

type VariableOption struct {
	Text     string `json:"text"`
	Value    string `json:"value"`
	Selected bool   `json:"selected"`
}

type DashboardVariable struct {
	Name       string              `json:"name"`
	Type       string              `json:"type"`
	Label      string              `json:"label"`
	Query      string              `json:"query"`
	Multi      bool                `json:"multi"`
	IncludeAll bool                `json:"includeAll"`
	AllValue   string              `json:"allValue,omitempty"`
	Refresh    int                 `json:"refresh"`
	Options    []VariableOption    `json:"options,omitempty"`
	Datasource DashboardDatasource `json:"datasource"`
	Current    VariableCurrent     `json:"current"`
}

type VariableCurrent struct {
	Selected bool     `json:"selected"`
	Text     []string `json:"text"`
	Value    []string `json:"value"`
}

type Dashboard struct {
	UID           string              `json:"uid"`
	Title         string              `json:"title"`
	Tags          []string            `json:"tags"`
	Time          DashboardTime       `json:"time"`
	Timezone      string              `json:"timezone"`
	SchemaVersion int                 `json:"schemaVersion"`
	Version       int                 `json:"version"`
	Editable      bool                `json:"editable"`
	Panels        []DashboardPanel    `json:"panels"`
	Templating    DashboardTemplating `json:"templating"`
}

type DashboardTime struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type DashboardTemplating struct {
	List []DashboardVariable `json:"list"`
}

type DashboardDatasource struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

func main() {
	appURL := os.Getenv("APP_URL")
	grafanaURL := os.Getenv("GRAFANA_URL")

	if appURL == "" {
		appURL = "http://localhost:8081"
	}

	if grafanaURL == "" {
		grafanaURL = "http://localhost:3000"
	}

	handlers, err := fetchHandlers(appURL)
	if err != nil {
		panic(err)
	}

	dashboard := createDashboard(handlers)

	data, err := json.MarshalIndent(dashboard, "", "  ")
	if err != nil {
		panic(err)
	}

	if err := saveDashboard(data, "dashboard.json"); err != nil {
		panic(err)
	}

	fmt.Println("Dashboard saved to dashboard.json")

	if err := uploadDashboard(data, grafanaURL); err != nil {
		panic(err)
	}
}

func fetchHandlers(baseURL string) ([]HandlerInfo, error) {
	client := http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Get(baseURL + "/handlers")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch handlers: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"unexpected status code: %d",
			resp.StatusCode,
		)
	}

	var result HandlersResponse

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode handlers: %w", err)
	}

	return result.Handlers, nil
}

func createTrafficPanel() DashboardPanel {
	return DashboardPanel{
		ID:    1,
		Title: "HTTP Requests per Second",
		Type:  "timeseries",
		Datasource: DashboardDatasource{
			Type: "prometheus",
			UID:  "prometheus",
		},
		GridPos: GridPosition{
			H: 8,
			W: 12,
			X: 0,
			Y: 0,
		},
		Targets: []PanelTarget{
			{
				Expr: `sum(rate(http_server_requests_total{
									datacenter=~"$datacenter",
									pod_id=~"$pod_id",
									operations=~"$route"
								}[1m]))`,
				RefID: "A",
				LegendFormat: "All Requests",
			},
		},
	}
}


func createLatencyPanel() DashboardPanel {
	return DashboardPanel{
		ID:    2,
		Title: "HTTP Request Latency",
		Type:  "timeseries",
		Datasource: DashboardDatasource{
			Type: "prometheus",
			UID:  "prometheus",
		},
		GridPos: GridPosition{
			H: 8,
			W: 12,
			X: 12,
			Y: 0,
		},
		Targets: []PanelTarget{
			{
				Expr: `histogram_quantile(0.50,
					sum by (le) (
						rate(http_server_request_duration_seconds_bucket{
							datacenter=~"$datacenter",
							pod_id=~"$pod_id",
							operations=~"$route"
						}[5m])
					)
				)`,
				RefID:        "A",
				LegendFormat: "p50",
			},
			{
				Expr: `histogram_quantile(0.95,
					sum by (le) (
						rate(http_server_request_duration_seconds_bucket{
							datacenter=~"$datacenter",
							pod_id=~"$pod_id",
							operations=~"$route"
						}[5m])
					)
				)`,
				RefID:        "B",
				LegendFormat: "p95",
			},
			{
				Expr: `histogram_quantile(0.99,
					sum by (le) (
						rate(http_server_request_duration_seconds_bucket{
							datacenter=~"$datacenter",
							pod_id=~"$pod_id",
							operations=~"$route"
						}[5m])
					)
				)`,
				RefID:        "C",
				LegendFormat: "p99",
			},
		},
	}
}


func createErrorsPanel() DashboardPanel {
	return DashboardPanel{
		ID:    3,
		Title: "HTTP Errors per Second",
		Type:  "timeseries",
		Datasource: DashboardDatasource{
			Type: "prometheus",
			UID:  "prometheus",
		},
		GridPos: GridPosition{
			H: 8,
			W: 12,
			X: 0,
			Y: 8,
		},
		Targets: []PanelTarget{
			{
				Expr: `sum by (status_code) (
					rate(http_server_errors_total{
						datacenter=~"$datacenter",
						pod_id=~"$pod_id",
						operations=~"$route"
					}[1m])
				)`,
				RefID:        "A",
				LegendFormat: "{{status_code}}",
			},
		},
	}
}

func createPrometheusVariables() []DashboardVariable {
	return []DashboardVariable{
		{
			Name:       "datacenter",
			Type:       "query",
			Label:      "Datacenter",
			Query:      "label_values(http_server_requests_total, datacenter)",
			Multi:      true,
			IncludeAll: true,
			AllValue:   ".*",
			Refresh:    1,

			Datasource: DashboardDatasource{
				Type: "prometheus",
				UID:  "prometheus",
			},
			Current: VariableCurrent{
				Selected: true,
				Text:     []string{"All"},
				Value:    []string{"$__all"},
			},
		},
		{
			Name:       "pod_id",
			Type:       "query",
			Label:      "Pod",
			Query:      "label_values(http_server_requests_total{datacenter=~\"$datacenter\"}, pod_id)",
			Multi:      true,
			IncludeAll: true,
			AllValue:   ".*",
			Refresh:    1,

			Datasource: DashboardDatasource{
				Type: "prometheus",
				UID:  "prometheus",
			},
			Current: VariableCurrent{
				Selected: true,
				Text:     []string{"All"},
				Value:    []string{"$__all"},
			},
		},
	}
}

func createRouteVariable(handlers []HandlerInfo) DashboardVariable {
	return DashboardVariable{
		Name:       "route",
		Type:       "query",
		Label:      "Route",
		Query:      "label_values(http_server_requests_total, operations)",
		Refresh:    1,
		Multi:      true,
		IncludeAll: true,
		AllValue:   ".*",
		Datasource: DashboardDatasource{
			Type: "prometheus",
			UID:  "prometheus",
		},
		Current: VariableCurrent{
			Selected: true,
			Text:     []string{"All"},
			Value:    []string{"$__all"},
		},
	}
}

func createDashboard(
	handlers []HandlerInfo,
) Dashboard {
	variables := createPrometheusVariables()

	routeVariable := createRouteVariable(handlers)
	variables = append(variables, routeVariable)

	panels := []DashboardPanel{
		createTrafficPanel(),
		createLatencyPanel(),
		createErrorsPanel(),
	}

	return Dashboard{
		UID:   "gin-web-golden-signals",
		Title: "Gin Web Server - Some Golden Signals",
		Tags:  []string{"gin", "prometheus", "golden-signals"},
		Time: DashboardTime{
			From: "now-1h",
			To:   "now",
		},
		Timezone:      "browser",
		SchemaVersion: 39,
		Version:       1,
		Editable:      false,
		Panels:        panels,
		Templating: DashboardTemplating{
			List: variables,
		},
	}
}

func saveDashboard(data []byte, filename string) error {
	return os.WriteFile(filename, data, 0644)
}

func uploadDashboard(data []byte, grafanaURL string) error {
	payload := map[string]interface{}{
		"dashboard": json.RawMessage(data),
		"folderId":  0,
		"overwrite": true,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		grafanaURL+"/api/dashboards/db",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(
		os.Getenv("GRAFANA_USER"),
		os.Getenv("GRAFANA_PASSWORD"),
	)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	responseBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Grafana API error: %s", responseBody)
	}

	fmt.Println("Dashboard uploaded:", string(responseBody))
	return nil
}
