package dashboard

import (
	"fmt"
	"strconv"
)

const UID = "items-api-overview"
const DatasourceUID = "items-prometheus"

type Object = map[string]any

type Endpoint struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

func Build(endpoints []Endpoint) Object {
	panels := make([]Object, 0, 3*len(endpoints))
	for row, endpoint := range endpoints {
		panels = append(panels, endpointPanels(endpoint, row)...)
	}
	return Object{
		"uid": UID, "title": "Items API", "schemaVersion": 41,
		"editable": false, "timezone": "browser", "refresh": "5s",
		"time": Object{"from": "now-15m", "to": "now"},
		"tags": []string{"items-api"}, "panels": panels,
		"templating": Object{"list": []Object{
			variable("datacenter", "Датацентр", `label_values(items_api_requests_total{job="items-api"}, datacenter)`),
			variable("pod_id", "Под", `label_values(items_api_requests_total{job="items-api",datacenter=~"${datacenter:regex}"}, pod_id)`),
		}},
	}
}

func endpointPanels(endpoint Endpoint, row int) []Object {
	scope := `job="items-api",datacenter=~"${datacenter:regex}",pod_id=~"${pod_id:regex}"`
	scope += ",method=" + strconv.Quote(endpoint.Method) + ",route=" + strconv.Quote(endpoint.Path)
	counter := "items_api_requests_total{" + scope + "}"
	buckets := "items_api_request_duration_seconds_bucket{" + scope + "}"
	latency := make([]Object, 0, 3)
	for i, percentile := range []float64{.5, .95, .99} {
		expr := fmt.Sprintf("histogram_quantile(%g, sum by (le) (rate(%s[$__rate_interval])))", percentile, buckets)
		latency = append(latency, target(expr, fmt.Sprintf("p%.0f", percentile*100), i))
	}
	title := endpoint.Method + " " + endpoint.Path
	return []Object{
		panel(row*3+1, title+" · Время ответа", "s", 0, row, latency),
		panel(row*3+2, title+" · RPS", "reqps", 8, row, []Object{
			target("sum(rate("+counter+"[$__rate_interval]))", "Запросы/с", 0),
		}),
		panel(row*3+3, title+" · Ошибки", "reqps", 16, row, []Object{
			target(`sum by (status_code) (rate(items_api_requests_total{`+scope+`,status_code=~"4..|5.."}[$__rate_interval]))`, "{{status_code}}", 0),
		}),
	}
}

func datasource() Object {
	return Object{"type": "prometheus", "uid": DatasourceUID}
}

func target(expression, legend string, index int) Object {
	return Object{"refId": string(rune('A' + index)), "expr": expression,
		"legendFormat": legend, "range": true, "datasource": datasource()}
}

func panel(id int, title, unit string, x, row int, targets []Object) Object {
	return Object{
		"id": id, "title": title, "type": "timeseries", "datasource": datasource(),
		"gridPos": Object{"x": x, "y": row * 8, "w": 8, "h": 8}, "targets": targets,
		"fieldConfig": Object{"defaults": Object{
			"unit": unit, "min": 0, "color": Object{"mode": "palette-classic"},
			"custom": Object{"drawStyle": "line", "lineWidth": 2, "fillOpacity": 8, "showPoints": "never"},
		}, "overrides": []Object{}},
		"options": Object{
			"legend":  Object{"displayMode": "list", "placement": "bottom"},
			"tooltip": Object{"mode": "multi", "sort": "desc"},
		},
	}
}

func variable(name, label, query string) Object {
	return Object{
		"name": name, "label": label, "type": "query", "datasource": datasource(),
		"query": query, "definition": query, "refresh": 1, "sort": 1,
		"multi": true, "includeAll": true, "allValue": ".*",
		"current": Object{"text": "All", "value": "$__all"}, "options": []Object{},
	}
}
