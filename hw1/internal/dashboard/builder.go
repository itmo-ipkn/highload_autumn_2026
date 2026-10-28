package dashboard

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

var (
	datasource = map[string]any{"type": "prometheus", "uid": "prometheus-ds"}

	datacenter = map[string]any{
		"current": map[string]any{
			"selected": true,
			"text":     []string{"All"},
			"value":    []string{"$__all"},
		},
		"datasource": datasource,
		"definition": "label_values(http_server_requests_count, datacenter)",
		"hide":       0,
		"includeAll": true,
		"label":      "Datacenter",
		"multi":      true,
		"name":       "datacenter",
		"options":    []any{},
		"query": map[string]any{
			"query": "label_values(http_server_requests_count, datacenter)",
			"refId": "StandardVariableQuery",
		},
		"refresh":     1,
		"regex":       "",
		"skipUrlSync": false,
		"type":        "query",
	}

	podID = map[string]any{
		"current": map[string]any{
			"selected": true,
			"text":     []string{"All"},
			"value":    []string{"$__all"},
		},
		"datasource": datasource,
		"definition": "label_values(http_server_requests_count, pod_id)",
		"hide":       0,
		"includeAll": true,
		"label":      "Pod ID",
		"multi":      true,
		"name":       "pod_id",
		"options":    []any{},
		"query": map[string]any{
			"query": "label_values(http_server_requests_count, pod_id)",
			"refId": "StandardVariableQuery",
		},
		"refresh":     1,
		"regex":       "",
		"skipUrlSync": false,
		"type":        "query",
	}
)

func buildMatchers(handlers []HandlerInfo) (string, string) {
	methods := map[string]struct{}{}
	paths := map[string]struct{}{}

	for _, h := range handlers {
		methods[h.Method] = struct{}{}
		paths[h.Path] = struct{}{}
	}

	ms := slices.Collect(maps.Keys(methods))
	ps := slices.Collect(maps.Keys(paths))

	return strings.Join(ms, "|"), strings.Join(ps, "|")
}

func getTotalOverride(totalName string) map[string]any {
	return map[string]any{
		"matcher": map[string]any{
			"id":      "byName",
			"options": totalName,
		},
		"properties": []map[string]any{
			{
				"id":    "custom.lineStyle",
				"value": map[string]any{"fill": "dash", "dash": []int{10, 10}},
			},
			{
				"id":    "custom.lineWidth",
				"value": 2,
			},
		},
	}
}

func Build(handlers []HandlerInfo) map[string]any {
	methods, paths := buildMatchers(handlers)

	commonParams := fmt.Sprintf(`method=~"%s", path=~"%s", datacenter=~"$datacenter", pod_id=~"$pod_id"`, methods, paths)
	latencyPanel := map[string]any{
		"id":      1,
		"title":   "Latency (p50, p95, p99)",
		"type":    "timeseries",
		"gridPos": map[string]any{"h": 10, "w": 12, "x": 0, "y": 0},
		"targets": []map[string]any{
			{
				"datasource":   datasource,
				"expr":         fmt.Sprintf("histogram_quantile(0.50, sum by (method, path, le) (rate(http_server_requests_latency_bucket{%s}[$__rate_interval])))", commonParams),
				"legendFormat": "p50 {{method}} {{path}}",
			},
			{
				"datasource":   datasource,
				"expr":         fmt.Sprintf("histogram_quantile(0.95, sum by (method, path, le) (rate(http_server_requests_latency_bucket{%s}[$__rate_interval])))", commonParams),
				"legendFormat": "p95 {{method}} {{path}}",
			},
			{
				"datasource":   datasource,
				"expr":         fmt.Sprintf("histogram_quantile(0.99, sum by (method, path, le) (rate(http_server_requests_latency_bucket{%s}[$__rate_interval])))", commonParams),
				"legendFormat": "p99 {{method}} {{path}}",
			},
		},
		"fieldConfig": map[string]any{
			"defaults": map[string]any{
				"unit": "s",
			},
		},
		"options": map[string]any{
			"legend": map[string]any{
				"displayMode": "table",
				"placement":   "bottom",
				"calcs":       []string{"max", "lastNotNull"},
				"sortBy":      "Max",
				"sortDesc":    true,
			},
		},
	}

	rpsPanel := map[string]any{
		"id":      2,
		"title":   "Throughput (RPS)",
		"type":    "timeseries",
		"gridPos": map[string]any{"h": 10, "w": 12, "x": 12, "y": 0},
		"targets": []map[string]any{
			{
				"datasource":   datasource,
				"expr":         fmt.Sprintf("sum by (method, path) (rate(http_server_requests_count{%s}[$__rate_interval]))", commonParams),
				"legendFormat": "{{method}} {{path}}",
			},
			{
				"datasource":   datasource,
				"expr":         fmt.Sprintf("sum(rate(http_server_requests_count{%s}[$__rate_interval]))", commonParams),
				"legendFormat": "Total",
			},
		},
		"fieldConfig": map[string]any{
			"defaults": map[string]any{
				"unit":     "reqps",
				"min":      0,
				"decimals": 0,
			},
			"overrides": []map[string]any{
				getTotalOverride("Total"),
			},
		},
		"options": map[string]any{
			"legend": map[string]any{
				"displayMode": "table",
				"placement":   "bottom",
				"calcs":       []string{"max", "lastNotNull"},
				"sortBy":      "Max",
				"sortDesc":    true,
			},
		},
	}

	error5xxParams := fmt.Sprintf(`method=~"%s", path=~"%s", status_code=~"5.*", datacenter=~"$datacenter", pod_id=~"$pod_id"`, methods, paths)
	errors5xxPanel := map[string]any{
		"id":      3,
		"title":   "Server Errors (5xx)",
		"type":    "timeseries",
		"gridPos": map[string]any{"h": 10, "w": 12, "x": 0, "y": 10},
		"targets": []map[string]any{
			{
				"datasource":   datasource,
				"expr":         fmt.Sprintf("sum by (method, path, status_code) (rate(http_server_requests_count{%s}[$__rate_interval]))", error5xxParams),
				"legendFormat": "{{method}} {{path}} {{status_code}}",
			},
			{
				"datasource":   datasource,
				"expr":         fmt.Sprintf("sum(rate(http_server_requests_count{%s}[$__rate_interval]))", error5xxParams),
				"legendFormat": "Total 5xx",
			},
		},
		"fieldConfig": map[string]any{
			"defaults": map[string]any{
				"unit":     "reqps",
				"min":      0,
				"decimals": 0,
			},
			"overrides": []map[string]any{
				getTotalOverride("Total 5xx"),
			},
		},
		"options": map[string]any{
			"legend": map[string]any{
				"displayMode": "table",
				"placement":   "bottom",
				"calcs":       []string{"max", "lastNotNull"},
				"sortBy":      "Max",
				"sortDesc":    true,
			},
		},
	}

	error4xxParams := fmt.Sprintf(`method=~"%s", path=~"%s", status_code=~"4.*", datacenter=~"$datacenter", pod_id=~"$pod_id"`, methods, paths)
	errors4xxPanel := map[string]any{
		"id":      4,
		"title":   "Client Errors (4xx)",
		"type":    "timeseries",
		"gridPos": map[string]any{"h": 10, "w": 12, "x": 12, "y": 10},
		"targets": []map[string]any{
			{
				"datasource":   datasource,
				"expr":         fmt.Sprintf("sum by (method, path, status_code) (rate(http_server_requests_count{%s}[$__rate_interval]))", error4xxParams),
				"legendFormat": "{{method}} {{path}} {{status_code}}",
			},
			{
				"datasource":   datasource,
				"expr":         fmt.Sprintf("sum(rate(http_server_requests_count{%s}[$__rate_interval]))", error4xxParams),
				"legendFormat": "Total 4xx",
			},
		},
		"fieldConfig": map[string]any{
			"defaults": map[string]any{
				"unit":     "reqps",
				"min":      0,
				"decimals": 0,
			},
			"overrides": []map[string]any{
				getTotalOverride("Total 4xx"),
			},
		},
		"options": map[string]any{
			"legend": map[string]any{
				"displayMode": "table",
				"placement":   "bottom",
				"calcs":       []string{"max", "lastNotNull"},
				"sortBy":      "Max",
				"sortDesc":    true,
			},
		},
	}

	dashboard := map[string]any{
		"editable":             false,
		"fiscalYearStartMonth": 0,
		"graphTooltip":         1,
		"liveNow":              false,
		"name":                 "My Service Metrics",
		"panels":               []any{latencyPanel, rpsPanel, errors5xxPanel, errors4xxPanel},
		"schemaVersion":        38,
		"style":                "dark",
		"tags":                 []string{"autogenerated", "microservice"},
		"time": map[string]any{
			"from": "now-15m",
			"to":   "now",
		},
		"timepicker": map[string]any{},
		"timezone":   "",
		"title":      "My Service Metrics",
		"uid":        "my-service-metrics",
		"version":    1,
		"weekStart":  "",
		"templating": map[string]any{
			"list": []map[string]any{
				datacenter,
				podID,
			},
		},
	}

	return map[string]any{
		"dashboard": dashboard,
		"overwrite": true,
	}
}
