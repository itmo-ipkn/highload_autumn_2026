package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/grafana/grafana-foundation-sdk/go/common"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/prometheus"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

const (
	datasourceUID     = "prometheus-main"
	dashboardUID      = "userver-crud-monitoring"
	maxDiscoveryBytes = 1 << 20
)

var httpMethod = regexp.MustCompile(`^[A-Z]+$`)

type Handler struct {
	Name   string `json:"name"`
	Method string `json:"method"`
	Path   string `json:"path"`
}

type HandlersResponse struct {
	Handlers []Handler `json:"handlers"`
}

func fetchHandlers(client *http.Client, baseURL string) ([]Handler, error) {
	url := strings.TrimRight(baseURL, "/") + "/handlers"

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetch handlers: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("/handlers returned %s", resp.Status)
	}

	var result HandlersResponse
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxDiscoveryBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read handlers: %w", err)
	}
	if len(body) > maxDiscoveryBytes {
		return nil, fmt.Errorf("handlers response exceeds %d bytes", maxDiscoveryBytes)
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decode handlers: %w", err)
	}
	if err := validateHandlers(result.Handlers); err != nil {
		return nil, err
	}

	return result.Handlers, nil
}

func validateHandlers(handlers []Handler) error {
	if len(handlers) == 0 {
		return fmt.Errorf("no business handlers found")
	}
	routes := make(map[string]bool)
	for _, h := range handlers {
		if strings.TrimSpace(h.Name) == "" || !httpMethod.MatchString(h.Method) ||
			!strings.HasPrefix(h.Path, "/") || strings.ContainsAny(h.Path, "\r\n\t?#") {
			return fmt.Errorf("invalid handler metadata: name=%q method=%q path=%q", h.Name, h.Method, h.Path)
		}
		key := h.Method + " " + h.Path
		if routes[key] {
			return fmt.Errorf("duplicate handler metadata: %s (%s)", h.Name, key)
		}
		routes[key] = true
	}
	return nil
}

func buildDashboard(handlers []Handler) (dashboard.Dashboard, error) {
	if err := validateHandlers(handlers); err != nil {
		return dashboard.Dashboard{}, err
	}
	dsType := "prometheus"
	dsUID := datasourceUID

	ds := common.DataSourceRef{
		Type: &dsType,
		Uid:  &dsUID,
	}

	filters := `datacenter=~"${datacenter:regex}",pod_id=~"${pod_id:regex}"`

	builder := dashboard.NewDashboardBuilder("Userver CRUD Monitoring").
		Uid(dashboardUID).
		Description("Automatically discovered HTTP handlers. RPS and errors are summed across the selected pods and datacenters; latency quantiles are computed from their combined histogram buckets.").
		Tags([]string{"userver", "prometheus", "generated"}).
		Refresh("5s").
		Time("now-15m", "now").
		Readonly()

	for _, variable := range []string{"datacenter", "pod_id"} {
		selector := `http_request_duration_seconds_count{job="userver"}`
		if variable == "pod_id" {
			selector = `http_request_duration_seconds_count{job="userver",datacenter=~"${datacenter:regex}"}`
		}
		query := fmt.Sprintf(
			`label_values(%s, %s)`, selector, variable,
		)
		allText, allValue := "All", "$__all"

		builder.WithVariable(
			dashboard.NewQueryVariableBuilder(variable).
				Label(variable).
				Datasource(ds).
				Query(dashboard.StringOrMap{String: &query}).
				Multi(true).
				IncludeAll(true).
				AllValue(".*").
				Refresh(dashboard.VariableRefreshOnTimeRangeChanged).
				Sort(dashboard.VariableSortAlphabeticalAsc).
				Current(dashboard.VariableOption{
					Text:  dashboard.StringOrArrayOfString{String: &allText},
					Value: dashboard.StringOrArrayOfString{String: &allValue},
				}),
		)
	}

	for index, h := range handlers {
		y := uint32(index * 8)
		panelID := uint32(index*3 + 1)

		latency := timeseries.NewPanelBuilder().
			Title(fmt.Sprintf("%s %s - Latency", h.Method, h.Path)).
			Description("p50, p95 and p99 of request latency, aggregated over selected instances before calculating each quantile.").
			Id(panelID).
			GridPos(dashboard.GridPos{X: 0, Y: y, W: 8, H: 8}).
			Datasource(ds).
			Unit("s").
			Min(0)

		for _, p := range []int{50, 95, 99} {
			expr := fmt.Sprintf(
				`histogram_quantile(%.2f, sum by (le) (rate(http_request_duration_seconds_bucket{job="userver",method=%q,route=%q,%s}[$__rate_interval])))`,
				float64(p)/100, h.Method, h.Path, filters,
			)

			latency.WithTarget(
				prometheus.NewDataqueryBuilder().
					RefId(fmt.Sprintf("P%d", p)).
					Expr(expr).
					Range().
					LegendFormat(fmt.Sprintf("p%d", p)),
			)
		}

		builder.WithPanel(latency)

		rpsExpr := fmt.Sprintf(
			`sum(rate(http_request_total{job="userver",method=%q,route=%q,%s}[$__rate_interval]))`,
			h.Method, h.Path, filters,
		)

		builder.WithPanel(
			timeseries.NewPanelBuilder().
				Title(fmt.Sprintf("%s %s - RPS", h.Method, h.Path)).
				Description("Total requests per second across all selected pods and datacenters.").
				Id(panelID + 1).
				GridPos(dashboard.GridPos{X: 8, Y: y, W: 8, H: 8}).
				Datasource(ds).
				Unit("reqps").
				Min(0).
				WithTarget(
					prometheus.NewDataqueryBuilder().
						RefId("A").
						Expr(rpsExpr).
						Range().
						LegendFormat("RPS"),
				),
		)

		errorsExpr := fmt.Sprintf(
			`sum by (status) (rate(http_request_errors_total{job="userver",method=%q,route=%q,status=~"4..|5..",%s}[$__rate_interval]))`,
			h.Method, h.Path, filters,
		)

		builder.WithPanel(
			timeseries.NewPanelBuilder().
				Title(fmt.Sprintf("%s %s - Errors", h.Method, h.Path)).
				Description("Errors per second, grouped by exact HTTP response code (4xx and 5xx), summed across selected instances.").
				Id(panelID + 2).
				GridPos(dashboard.GridPos{X: 16, Y: y, W: 8, H: 8}).
				Datasource(ds).
				Unit("reqps").
				Min(0).
				WithTarget(
					prometheus.NewDataqueryBuilder().
						RefId("A").
						Expr(errorsExpr).
						Range().
						LegendFormat("HTTP {{status}}"),
				),
		)
	}

	return builder.Build()
}

func uploadDashboard(
	client *http.Client,
	grafanaURL, username, password string,
	dash dashboard.Dashboard,
) error {
	payload := struct {
		Dashboard dashboard.Dashboard `json:"dashboard"`
		Overwrite bool                `json:"overwrite"`
	}{
		Dashboard: dash,
		Overwrite: true,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	url := strings.TrimRight(grafanaURL, "/") + "/api/dashboards/db"

	req, err := http.NewRequest(
		http.MethodPost, url, bytes.NewReader(body),
	)
	if err != nil {
		return err
	}

	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		details, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("grafana returned %s: %s",
			resp.Status, string(details))
	}

	return nil
}

func main() {
	appURL := os.Getenv("APP_URL")
	grafanaURL := os.Getenv("GRAFANA_URL")
	username := os.Getenv("GRAFANA_ADMIN_USER")
	password := os.Getenv("GRAFANA_ADMIN_PASSWORD")

	if appURL == "" || grafanaURL == "" ||
		username == "" || password == "" {
		log.Fatal("required environment variables are missing")
	}

	client := &http.Client{Timeout: 15 * time.Second}

	handlers, err := fetchHandlers(client, appURL)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Discovered %d handlers", len(handlers))

	dash, err := buildDashboard(handlers)
	if err != nil {
		log.Fatal(err)
	}

	if err := uploadDashboard(
		client, grafanaURL, username, password, dash,
	); err != nil {
		log.Fatal(err)
	}

	log.Printf("Dashboard created: %s", dashboardUID)
}
