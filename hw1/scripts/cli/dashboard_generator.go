package main

import (
	"fmt"

	"github.com/nikita/highload_autumn_2026/hw1/scripts/cli/dto"
)

const (
	panelWidth  = 8
	panelHeight = 8
	panelPerRow = 3
)

func generateDashboard(handlers []dto.HandlerInfo, uid string, promUID string, refresh string) dto.Dashboard {
	time := dto.Time{
		From: "now-15m",
		To:   "now",
	}

	templating := generateTemplating(promUID)

	dashboard := dto.Dashboard{
		UID:   uid,
		Title: "Homework1 dashboard",
		Tags: []string{
			"highload",
			"hw1",
		},
		Timezone:      "browser",
		SchemaVersion: 1,
		Version:       1,
		Refresh:       refresh,
		Time:          time,
		Editable:      false,
		Panels:        make([]dto.Panel, 0, len(handlers)*3),
		Templating:    templating,
	}

	startID := 1
	for i, handler := range handlers {
		handlerPanels := generatePanels(handler, promUID, startID)
		for j := range handlerPanels {
			handlerPanels[j].GridPos = dto.GridPosition{
				X: j * panelWidth,
				Y: i * panelHeight,
				W: panelWidth,
				H: panelHeight,
			}
		}

		dashboard.Panels = append(dashboard.Panels, handlerPanels...)

		startID += len(handlerPanels)
	}

	return dashboard
}

func generatePanels(handler dto.HandlerInfo, promUID string, id int) []dto.Panel {
	panels := make([]dto.Panel, 3)

	panels[0] = generateRpsPanel(handler, promUID, id)
	panels[1] = generateLatencyPanel(handler, promUID, id+1)
	panels[2] = generateErrorsPanel(handler, promUID, id+2)

	return panels
}

func generateRpsPanel(handler dto.HandlerInfo, promUID string, id int) dto.Panel {
	title := fmt.Sprintf("%s %s %s", handler.Method, handler.URI, "RPS Panel")
	description := fmt.Sprintf("The panel endpoint %s %s about RPS", handler.Method, handler.URI)
	datasource := dto.Datasource{
		Type: "prometheus",
		UID:  promUID,
	}

	commonLables := buildCommonLables(handler)
	generalRps := dto.Target{
		RefID:        "A",
		Expr:         fmt.Sprintf("sum(rate(http_server_requests_seconds_count{%s}[$__rate_interval]))", commonLables),
		LegendFormat: "general_rps",
		Datasource:   datasource,
	}

	successRps := dto.Target{
		RefID:        "B",
		Expr:         fmt.Sprintf("sum(rate(http_server_requests_seconds_count{status=~\"2..\",%s}[$__rate_interval]))", commonLables),
		LegendFormat: "success_rps",
		Datasource:   datasource,
	}

	fieldConfig := dto.FieldConfig{
		Defaults: dto.DefaultFieldConfig{
			Unit: "reqps",
			Min:  0,
			Custom: dto.CustomField{
				DrawStyle:   "line",
				LineWidth:   2,
				FillOpacity: 0,
				SpanNulls:   false,
			},
		},
		Overrides: []dto.Override{},
	}

	return dto.Panel{
		ID:          id,
		Type:        "timeseries",
		Title:       title,
		Description: description,
		Datasource:  datasource,
		Targets:     []dto.Target{generalRps, successRps},
		FieldConfig: fieldConfig,
		Options:     buildDefaulOptions(),
	}
}

func generateLatencyPanel(handler dto.HandlerInfo, promUID string, id int) dto.Panel {
	title := fmt.Sprintf("%s %s %s", handler.Method, handler.URI, "Latency Panel")
	description := fmt.Sprintf("The panel endpoint %s %s about latency", handler.Method, handler.URI)
	datasource := dto.Datasource{
		Type: "prometheus",
		UID:  promUID,
	}

	commonLables := buildCommonLables(handler)
	meanLatency := dto.Target{
		RefID: "A",
		Expr: fmt.Sprintf(
			"sum(rate(http_server_requests_seconds_sum{%s}[$__rate_interval])) / sum(rate(http_server_requests_seconds_count{%s}[$__rate_interval]))",
			commonLables,
			commonLables,
		),
		LegendFormat: "mean_latency",
		Datasource:   datasource,
	}

	latencyP50 := dto.Target{
		RefID:        "B",
		Expr:         fmt.Sprintf("histogram_quantile(0.50, sum by (le) (rate(http_server_requests_seconds_bucket{%s}[$__rate_interval])))", commonLables),
		LegendFormat: "latency_p50",
		Datasource:   datasource,
	}

	latencyP95 := dto.Target{
		RefID:        "C",
		Expr:         fmt.Sprintf("histogram_quantile(0.95, sum by (le) (rate(http_server_requests_seconds_bucket{%s}[$__rate_interval])))", commonLables),
		LegendFormat: "latency_p95",
		Datasource:   datasource,
	}

	latencyP99 := dto.Target{
		RefID:        "D",
		Expr:         fmt.Sprintf("histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{%s}[$__rate_interval])))", commonLables),
		LegendFormat: "latency_p99",
		Datasource:   datasource,
	}

	fieldConfig := dto.FieldConfig{
		Defaults: dto.DefaultFieldConfig{
			Unit: "s",
			Min:  0,
			Custom: dto.CustomField{
				DrawStyle:   "line",
				LineWidth:   2,
				FillOpacity: 0,
				SpanNulls:   false,
			},
		},
		Overrides: []dto.Override{},
	}

	return dto.Panel{
		ID:          id,
		Type:        "timeseries",
		Title:       title,
		Description: description,
		Datasource:  datasource,
		Targets:     []dto.Target{meanLatency, latencyP50, latencyP95, latencyP99},
		FieldConfig: fieldConfig,
		Options:     buildDefaulOptions(),
	}
}

func generateErrorsPanel(handler dto.HandlerInfo, promUID string, id int) dto.Panel {
	title := fmt.Sprintf("%s %s %s", handler.Method, handler.URI, "Errors Panel")
	description := fmt.Sprintf("The panel endpoint %s %s about errors", handler.Method, handler.URI)
	datasource := dto.Datasource{
		Type: "prometheus",
		UID:  promUID,
	}

	commonLables := buildCommonLables(handler)
	errors := dto.Target{
		RefID:        "A",
		Expr:         fmt.Sprintf("(sum(rate(http_server_requests_seconds_count{status=~\"[45]..\",%s}[$__rate_interval])) or vector(0)) / sum(rate(http_server_requests_seconds_count{%s}[$__rate_interval]))", commonLables, commonLables),
		LegendFormat: "error_rate",
		Datasource:   datasource,
	}

	max := 1
	fieldConfig := dto.FieldConfig{
		Defaults: dto.DefaultFieldConfig{
			Unit: "percentunit",
			Min:  0,
			Max:  &max,
			Custom: dto.CustomField{
				DrawStyle:   "line",
				LineWidth:   2,
				FillOpacity: 0,
				SpanNulls:   false,
			},
		},
		Overrides: []dto.Override{},
	}

	return dto.Panel{
		ID:          id,
		Type:        "timeseries",
		Title:       title,
		Description: description,
		Datasource:  datasource,
		Targets:     []dto.Target{errors},
		FieldConfig: fieldConfig,
		Options:     buildDefaulOptions(),
	}
}

func generateTemplating(promUID string) dto.Templating {
	datasource := dto.Datasource{
		Type: "prometheus",
		UID:  promUID,
	}

	varByDatacenter := dto.Variable{
		Name:       "datacenter",
		Label:      "Datacenter",
		Type:       "query",
		Datasource: datasource,
		Query:      `label_values(http_server_requests_seconds_count, datacenter)`,
		Multi:      true,
		IncludeAll: true,
		AllValue:   ".*",
		Refresh:    1,
		Sort:       1,
		Current: dto.VariableCurrent{
			Selected: true,
			Text:     "All",
			Value:    "$__all",
		},
	}

	varByPod := dto.Variable{
		Name:       "pod",
		Label:      "Pod",
		Type:       "query",
		Datasource: datasource,
		Query:      `label_values(http_server_requests_seconds_count{datacenter=~"${datacenter:regex}"}, pod_id)`,
		Multi:      true,
		IncludeAll: true,
		AllValue:   ".*",
		Refresh:    1,
		Sort:       1,
		Current: dto.VariableCurrent{
			Selected: true,
			Text:     "All",
			Value:    "$__all",
		},
	}

	return dto.Templating{
		List: []dto.Variable{varByDatacenter, varByPod},
	}
}

func buildCommonLables(handler dto.HandlerInfo) string {
	return fmt.Sprintf(
		`method=%q,uri=%q,datacenter=~"${datacenter:regex}",pod_id=~"${pod:regex}"`,
		handler.Method,
		handler.URI,
	)
}

func buildDefaulOptions() dto.Option {
	return dto.Option{
		Legend: dto.Legend{
			DisplayMode: "list",
			Placement:   "bottom",
		},
		Tooltip: dto.Tooltip{
			Mode: "multi",
		},
	}

}
