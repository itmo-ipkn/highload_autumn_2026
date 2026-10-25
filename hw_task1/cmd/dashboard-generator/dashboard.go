package main

import (
	"fmt"
	"strconv"
)

const (
	dashboardUID   = "hw1-handlers"
	dashboardTitle = "HW1 — Handlers"
	selector       = `dc=~"$dc",pod=~"$pod"`
)

type datasourceRef struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type gridPos struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

type target struct {
	RefID        string        `json:"refId"`
	Datasource   datasourceRef `json:"datasource"`
	Expr         string        `json:"expr"`
	LegendFormat string        `json:"legendFormat"`
}

type panel struct {
	ID          int            `json:"id"`
	Type        string         `json:"type"`
	Title       string         `json:"title"`
	GridPos     gridPos        `json:"gridPos"`
	Datasource  *datasourceRef `json:"datasource,omitempty"`
	Targets     []target       `json:"targets,omitempty"`
	FieldConfig map[string]any `json:"fieldConfig,omitempty"`
	Options     map[string]any `json:"options,omitempty"`
	Collapsed   *bool          `json:"collapsed,omitempty"`
	Panels      []panel        `json:"panels,omitempty"`
}

type variable struct {
	Type       string         `json:"type"`
	Name       string         `json:"name"`
	Label      string         `json:"label"`
	Datasource datasourceRef  `json:"datasource"`
	Query      string         `json:"query"`
	Definition string         `json:"definition"`
	Refresh    int            `json:"refresh"`
	Multi      bool           `json:"multi"`
	IncludeAll bool           `json:"includeAll"`
	AllValue   string         `json:"allValue"`
	Sort       int            `json:"sort"`
	Current    map[string]any `json:"current"`
}

type dashboard struct {
	UID           string            `json:"uid"`
	Title         string            `json:"title"`
	Tags          []string          `json:"tags"`
	Editable      bool              `json:"editable"`
	SchemaVersion int               `json:"schemaVersion"`
	Refresh       string            `json:"refresh"`
	Time          map[string]string `json:"time"`
	Templating    map[string]any    `json:"templating"`
	Panels        []panel           `json:"panels"`
}

func buildDashboard(handlers []string, dsUID string) dashboard {
	ds := datasourceRef{Type: "prometheus", UID: dsUID}

	d := dashboard{
		UID:           dashboardUID,
		Title:         dashboardTitle,
		Tags:          []string{"hw1", "generated"},
		Editable:      false,
		SchemaVersion: 41,
		Refresh:       "5s",
		Time:          map[string]string{"from": "now-15m", "to": "now"},
		Templating: map[string]any{"list": []variable{
			labelVariable(ds, "dc", "Datacenter", "label_values(http_request_duration_seconds_count, dc)"),
			labelVariable(ds, "pod", "Pod", `label_values(http_request_duration_seconds_count{dc=~"$dc"}, pod)`),
		}},
	}

	id, y := 1, 0
	addRow := func(title string, panels []panel) {
		collapsed := false
		d.Panels = append(d.Panels, panel{
			ID: id, Type: "row", Title: title, Collapsed: &collapsed, Panels: []panel{},
			GridPos: gridPos{H: 1, W: 24, X: 0, Y: y},
		})
		id, y = id+1, y+1

		for i, p := range panels {
			p.ID = id
			p.GridPos = gridPos{H: 8, W: 8, X: i * 8, Y: y}
			d.Panels = append(d.Panels, p)
			id++
		}
		y += 8
	}

	addRow("Overview", overviewPanels(ds))
	for _, h := range handlers {
		addRow(h, handlerPanels(ds, h))
	}
	return d
}

func overviewPanels(ds datasourceRef) []panel {
	requests := fmt.Sprintf("http_requests_total{%s}", selector)
	errors := fmt.Sprintf(`http_requests_total{code=~"4..|5..",%s}`, selector)
	bucket := fmt.Sprintf("http_request_duration_seconds_bucket{%s}", selector)

	return []panel{
		timeseries(ds, "RPS by handler", "reqps", "", []target{
			{
				RefID: "A", Datasource: ds, LegendFormat: "{{handler}}",
				Expr: fmt.Sprintf("sum by (handler) (rate(%s[$__rate_interval]))", requests),
			},
			{
				RefID: "B", Datasource: ds, LegendFormat: "total",
				Expr: fmt.Sprintf("sum(rate(%s[$__rate_interval]))", requests),
			},
		}),
		timeseries(ds, "Error rate by handler", "percent", "0", []target{{
			RefID: "A", Datasource: ds, LegendFormat: "{{handler}}",
			Expr: fmt.Sprintf("100 * sum by (handler) (rate(%s[$__rate_interval])) / sum by (handler) (rate(%s[$__rate_interval]))", errors, requests),
		}}),
		timeseries(ds, "p99 latency by handler", "s", "", []target{{
			RefID: "A", Datasource: ds, LegendFormat: "{{handler}}",
			Expr: fmt.Sprintf("histogram_quantile(0.99, sum by (le, handler) (rate(%s[$__rate_interval])))", bucket),
		}}),
	}
}

func labelVariable(ds datasourceRef, name, label, query string) variable {
	return variable{
		Type: "query", Name: name, Label: label, Datasource: ds,
		Query: query, Definition: query,
		Refresh: 2, Multi: true, IncludeAll: true, AllValue: ".*", Sort: 1,
		Current: map[string]any{"text": []string{"All"}, "value": []string{"$__all"}},
	}
}

func handlerPanels(ds datasourceRef, handler string) []panel {
	h := "handler=" + strconv.Quote(handler)
	bucket := fmt.Sprintf("http_request_duration_seconds_bucket{%s,%s}", h, selector)
	requests := fmt.Sprintf("http_requests_total{%s,%s}", h, selector)
	errors := fmt.Sprintf(`http_requests_total{%s,code=~"4..|5..",%s}`, h, selector)

	quantiles := []struct{ ref, q, legend string }{
		{"A", "0.5", "p50"},
		{"B", "0.95", "p95"},
		{"C", "0.99", "p99"},
	}
	var latency []target
	for _, q := range quantiles {
		latency = append(latency, target{
			RefID:        q.ref,
			Datasource:   ds,
			Expr:         fmt.Sprintf("histogram_quantile(%s, sum by (le) (rate(%s[$__rate_interval])))", q.q, bucket),
			LegendFormat: q.legend,
		})
	}

	return []panel{
		timeseries(ds, "Latency", "s", "", latency),
		timeseries(ds, "RPS", "reqps", "", []target{{
			RefID: "A", Datasource: ds, LegendFormat: "rps",
			Expr: fmt.Sprintf("sum(rate(%s[$__rate_interval]))", requests),
		}}),
		timeseries(ds, "Errors", "reqps", "0", []target{{
			RefID: "A", Datasource: ds, LegendFormat: "{{code}}",
			Expr: fmt.Sprintf("sum by (code) (rate(%s[$__rate_interval]))", errors),
		}}),
	}
}

func timeseries(ds datasourceRef, title, unit, noValue string, targets []target) panel {
	defaults := map[string]any{"unit": unit, "min": 0}
	if noValue != "" {
		defaults["noValue"] = noValue
	}
	return panel{
		Type:        "timeseries",
		Title:       title,
		Datasource:  &ds,
		Targets:     targets,
		FieldConfig: map[string]any{"defaults": defaults, "overrides": []any{}},
		Options: map[string]any{
			"legend":  map[string]any{"displayMode": "list", "placement": "bottom", "showLegend": true},
			"tooltip": map[string]any{"mode": "multi", "sort": "desc"},
		},
	}
}
