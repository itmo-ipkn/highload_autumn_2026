package dashboard

import (
	"fmt"
	"strconv"
	"strings"
)

// Handler - ручка приложения из GET /handlers.
type Handler struct {
	Method string `json:"method"`
	Route  string `json:"route"`
}

type Options struct {
	UID           string
	Title         string
	DatasourceUID string
}

const (
	durationMetric = "http_server_request_duration_seconds_bucket"
	requestsMetric = "http_server_requests_total"
	panelHeight    = 8
	panelWidth     = 8
)

// Build собирает дашборд: на каждую ручку строка с latency, RPS и ошибками.
func Build(opts Options, handlers []Handler) Dashboard {
	ds := Datasource{Type: "prometheus", UID: opts.DatasourceUID}
	d := Dashboard{
		UID:           opts.UID,
		Title:         opts.Title,
		Tags:          []string{"generated"},
		Editable:      false,
		Refresh:       "5s",
		SchemaVersion: 41,
		Time:          TimeRange{From: "now-15m", To: "now"},
		Templating: Templating{List: []Variable{
			variable(ds, "datacenter", "Датацентр", fmt.Sprintf("label_values(%s, datacenter)", requestsMetric)),
			variable(ds, "pod", "Под", fmt.Sprintf(`label_values(%s{datacenter=~"$datacenter"}, pod)`, requestsMetric)),
		}},
	}

	id, y := 1, 0
	for _, h := range handlers {
		collapsed := false
		d.Panels = append(d.Panels, Panel{
			ID: id, Type: "row", Title: h.Method + " " + h.Route,
			GridPos: GridPos{H: 1, W: 24, Y: y}, Collapsed: &collapsed,
		})
		id++
		y++

		sel := selector(h)
		for i, p := range []Panel{
			timeseries("Latency", "s", latencyTargets(ds, sel)),
			timeseries("RPS", "reqps", []Target{{
				RefID: "A", Datasource: ds, LegendFormat: "rps",
				Expr: fmt.Sprintf("sum(rate(%s{%s}[$__rate_interval]))", requestsMetric, sel),
			}}),
			timeseries("Ошибки", "reqps", []Target{{
				RefID: "A", Datasource: ds, LegendFormat: "{{code}}",
				Expr: fmt.Sprintf(`sum by (code) (rate(%s{%s, code=~"4..|5.."}[$__rate_interval]))`, requestsMetric, sel),
			}}),
		} {
			p.ID = id
			p.Datasource = &ds
			p.GridPos = GridPos{H: panelHeight, W: panelWidth, X: i * panelWidth, Y: y}
			d.Panels = append(d.Panels, p)
			id++
		}
		y += panelHeight
	}
	return d
}

func variable(ds Datasource, name, label, query string) Variable {
	return Variable{
		Name: name, Label: label, Type: "query", Datasource: ds,
		Query: query, Definition: query,
		Refresh: 2, Multi: true, IncludeAll: true, AllValue: ".*", Sort: 1,
		Current: Current{Text: []string{"All"}, Value: []string{"$__all"}},
	}
}

// selector - матчеры ручки и выбранных в селекторах датацентров и подов.
func selector(h Handler) string {
	return strings.Join([]string{
		"method=" + strconv.Quote(h.Method),
		"route=" + strconv.Quote(h.Route),
		`datacenter=~"$datacenter"`,
		`pod=~"$pod"`,
	}, ", ")
}

// latencyTargets - перцентили по бакетам, просуммированным по всем подам.
func latencyTargets(ds Datasource, sel string) []Target {
	quantiles := []struct{ q, legend string }{{"0.5", "p50"}, {"0.95", "p95"}, {"0.99", "p99"}}
	out := make([]Target, 0, len(quantiles))
	for i, q := range quantiles {
		out = append(out, Target{
			RefID:        string(rune('A' + i)),
			Datasource:   ds,
			LegendFormat: q.legend,
			Expr:         fmt.Sprintf("histogram_quantile(%s, sum by (le) (rate(%s{%s}[$__rate_interval])))", q.q, durationMetric, sel),
		})
	}
	return out
}

func timeseries(title, unit string, targets []Target) Panel {
	zero := 0
	return Panel{
		Type: "timeseries", Title: title, Targets: targets,
		FieldConfig: &FieldConfig{Defaults: FieldDefaults{Unit: unit, Min: &zero}, Overrides: []any{}},
		// общий тултип: p50/p95/p99 и коды ошибок видны в одной точке
		Options: &PanelOptions{
			Tooltip: Tooltip{Mode: "multi", Sort: "desc"},
			Legend:  Legend{DisplayMode: "list", Placement: "bottom", ShowLegend: true},
		},
	}
}
