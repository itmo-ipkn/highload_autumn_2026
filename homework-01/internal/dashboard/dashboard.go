package dashboard

import (
	"fmt"
	"strconv"
)

const (
	requestsMetric = "http_requests_total"
	durationMetric = "http_request_duration_seconds_bucket"

	rowHeight   = 1
	panelHeight = 8
	panelWidth  = 8
)

var quantiles = []float64{0.5, 0.95, 0.99}

type Handler struct {
	Method string `json:"method"`
	Route  string `json:"route"`
}

type Options struct {
	UID           string
	Title         string
	DatasourceUID string
}

type Dashboard struct {
	UID           string     `json:"uid"`
	Title         string     `json:"title"`
	Tags          []string   `json:"tags"`
	Editable      bool       `json:"editable"`
	SchemaVersion int        `json:"schemaVersion"`
	Refresh       string     `json:"refresh"`
	Time          TimeRange  `json:"time"`
	Templating    Templating `json:"templating"`
	Panels        []Panel    `json:"panels"`
}

type TimeRange struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type Templating struct {
	List []Variable `json:"list"`
}

type Datasource struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type Variable struct {
	Name       string        `json:"name"`
	Label      string        `json:"label"`
	Type       string        `json:"type"`
	Datasource Datasource    `json:"datasource"`
	Query      VariableQuery `json:"query"`
	Definition string        `json:"definition"`
	Multi      bool          `json:"multi"`
	IncludeAll bool          `json:"includeAll"`
	Refresh    int           `json:"refresh"`
	Sort       int           `json:"sort"`
	Current    Current       `json:"current"`
}

type VariableQuery struct {
	Query string `json:"query"`
	RefID string `json:"refId"`
}

type Current struct {
	Text  []string `json:"text"`
	Value []string `json:"value"`
}

type GridPos struct {
	X int `json:"x"`
	Y int `json:"y"`
	W int `json:"w"`
	H int `json:"h"`
}

type Panel struct {
	ID          int          `json:"id"`
	Type        string       `json:"type"`
	Title       string       `json:"title"`
	GridPos     GridPos      `json:"gridPos"`
	Collapsed   *bool        `json:"collapsed,omitempty"`
	Datasource  *Datasource  `json:"datasource,omitempty"`
	Targets     []Target     `json:"targets,omitempty"`
	FieldConfig *FieldConfig `json:"fieldConfig,omitempty"`
	Options     *PanelOpts   `json:"options,omitempty"`
}

type Target struct {
	RefID        string     `json:"refId"`
	Datasource   Datasource `json:"datasource"`
	Expr         string     `json:"expr"`
	LegendFormat string     `json:"legendFormat"`
	Range        bool       `json:"range"`
}

type FieldConfig struct {
	Defaults  FieldDefaults `json:"defaults"`
	Overrides []any         `json:"overrides"`
}

type FieldDefaults struct {
	Unit   string         `json:"unit"`
	Min    *float64       `json:"min,omitempty"`
	Custom map[string]any `json:"custom,omitempty"`
}

type PanelOpts struct {
	Legend  Legend  `json:"legend"`
	Tooltip Tooltip `json:"tooltip"`
}

type Legend struct {
	DisplayMode string   `json:"displayMode"`
	Placement   string   `json:"placement"`
	ShowLegend  bool     `json:"showLegend"`
	Calcs       []string `json:"calcs"`
}

type Tooltip struct {
	Mode string `json:"mode"`
	Sort string `json:"sort"`
}

func Build(handlers []Handler, opts Options) Dashboard {
	ds := Datasource{Type: "prometheus", UID: opts.DatasourceUID}

	b := &builder{ds: ds}
	for _, h := range handlers {
		b.addHandler(h)
	}

	return Dashboard{
		UID:           opts.UID,
		Title:         opts.Title,
		Tags:          []string{"generated", "golden-signals"},
		Editable:      false,
		SchemaVersion: 41,
		Refresh:       "10s",
		Time:          TimeRange{From: "now-15m", To: "now"},
		Templating: Templating{List: []Variable{
			labelVariable(ds, "datacenter", "Datacenter", requestsMetric),
			labelVariable(ds, "pod", "Pod", requestsMetric+`{datacenter=~"$datacenter"}`),
		}},
		Panels: b.panels,
	}
}

func Selector(h Handler) string {
	return fmt.Sprintf(`method=%s, route=%s, datacenter=~"$datacenter", pod=~"$pod"`,
		strconv.Quote(h.Method), strconv.Quote(h.Route))
}

func LatencyExpr(h Handler, q float64) string {
	return fmt.Sprintf(`histogram_quantile(%s, sum by (le) (rate(%s{%s}[$__rate_interval])))`,
		formatFloat(q), durationMetric, Selector(h))
}

func RPSExpr(h Handler) string {
	return fmt.Sprintf(`sum(rate(%s{%s}[$__rate_interval]))`, requestsMetric, Selector(h))
}

func RPSByDatacenterExpr(h Handler) string {
	return fmt.Sprintf(`sum by (datacenter) (rate(%s{%s}[$__rate_interval]))`, requestsMetric, Selector(h))
}

func ErrorsExpr(h Handler) string {
	return fmt.Sprintf(`sum by (code) (rate(%s{%s, code=~"4..|5.."}[$__rate_interval]))`, requestsMetric, Selector(h))
}

type builder struct {
	ds     Datasource
	panels []Panel
	nextID int
	y      int
}

func (b *builder) id() int {
	b.nextID++
	return b.nextID
}

func (b *builder) addHandler(h Handler) {
	collapsed := false
	b.panels = append(b.panels, Panel{
		ID:        b.id(),
		Type:      "row",
		Title:     h.Method + " " + h.Route,
		GridPos:   GridPos{X: 0, Y: b.y, W: 24, H: rowHeight},
		Collapsed: &collapsed,
	})
	b.y += rowHeight

	latency := make([]Target, 0, len(quantiles))
	for i, q := range quantiles {
		latency = append(latency, b.target(refID(i), LatencyExpr(h, q), "p"+formatFloat(q*100)))
	}

	b.addPanel(0, "Latency", "s", latency)
	b.addPanel(1, "RPS", "reqps", []Target{
		b.target("A", RPSExpr(h), "total"),
		b.target("B", RPSByDatacenterExpr(h), "{{datacenter}}"),
	})
	b.addPanel(2, "Errors by code", "reqps", []Target{
		b.target("A", ErrorsExpr(h), "{{code}}"),
	})
	b.y += panelHeight
}

func (b *builder) addPanel(col int, title, unit string, targets []Target) {
	zero := 0.0
	ds := b.ds
	b.panels = append(b.panels, Panel{
		ID:         b.id(),
		Type:       "timeseries",
		Title:      title,
		GridPos:    GridPos{X: col * panelWidth, Y: b.y, W: panelWidth, H: panelHeight},
		Datasource: &ds,
		Targets:    targets,
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Unit: unit,
				Min:  &zero,
				Custom: map[string]any{
					"drawStyle":   "line",
					"lineWidth":   1,
					"fillOpacity": 10,
					"showPoints":  "never",
					"spanNulls":   true,
				},
			},
			Overrides: []any{},
		},
		Options: &PanelOpts{
			Legend:  Legend{DisplayMode: "list", Placement: "bottom", ShowLegend: true, Calcs: []string{}},
			Tooltip: Tooltip{Mode: "multi", Sort: "desc"},
		},
	})
}

func (b *builder) target(ref, expr, legend string) Target {
	return Target{RefID: ref, Datasource: b.ds, Expr: expr, LegendFormat: legend, Range: true}
}

func labelVariable(ds Datasource, name, label, series string) Variable {
	query := fmt.Sprintf("label_values(%s, %s)", series, name)
	return Variable{
		Name:       name,
		Label:      label,
		Type:       "query",
		Datasource: ds,
		Query:      VariableQuery{Query: query, RefID: "PrometheusVariableQueryEditor-VariableQuery"},
		Definition: query,
		Multi:      true,
		IncludeAll: true,
		Refresh:    2,
		Sort:       1,
		Current:    Current{Text: []string{"All"}, Value: []string{"$__all"}},
	}
}

func refID(i int) string {
	return string(rune('A' + i))
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}
