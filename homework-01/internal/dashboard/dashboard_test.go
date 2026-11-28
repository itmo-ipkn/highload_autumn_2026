package dashboard

import (
	"encoding/json"
	"strings"
	"testing"
)

var testHandlers = []Handler{
	{Method: "POST", Route: "/items"},
	{Method: "GET", Route: "/items/{id}"},
	{Method: "PUT", Route: "/items/{id}"},
}

var testOpts = Options{UID: "items", Title: "Items", DatasourceUID: "prometheus"}

func TestBuildLayout(t *testing.T) {
	d := Build(testHandlers, testOpts)

	if d.Editable {
		t.Fatal("dashboard must be non-editable")
	}
	if d.UID != "items" || d.Title != "Items" {
		t.Fatalf("unexpected uid/title %q/%q", d.UID, d.Title)
	}
	if want := len(testHandlers) * 4; len(d.Panels) != want {
		t.Fatalf("got %d panels, want %d", len(d.Panels), want)
	}

	ids := map[int]bool{}
	for i, p := range d.Panels {
		if ids[p.ID] {
			t.Fatalf("duplicate panel id %d", p.ID)
		}
		ids[p.ID] = true

		h := testHandlers[i/4]
		switch i % 4 {
		case 0:
			if p.Type != "row" || p.Title != h.Method+" "+h.Route {
				t.Fatalf("panel %d: want row %q, got %s %q", i, h.Method+" "+h.Route, p.Type, p.Title)
			}
		default:
			if p.Type != "timeseries" || p.Datasource == nil || p.Datasource.UID != "prometheus" {
				t.Fatalf("panel %d: unexpected %+v", i, p)
			}
			for _, tg := range p.Targets {
				if tg.Datasource.UID != "prometheus" {
					t.Fatalf("panel %d: target datasource %q", i, tg.Datasource.UID)
				}
			}
		}
	}
}

func TestBuildEmpty(t *testing.T) {
	d := Build(nil, testOpts)
	if len(d.Panels) != 0 || len(d.Templating.List) != 2 {
		t.Fatalf("unexpected dashboard for no handlers: %+v", d)
	}
}

func TestVariables(t *testing.T) {
	vars := Build(testHandlers, testOpts).Templating.List

	want := map[string]string{
		"datacenter": `label_values(http_requests_total, datacenter)`,
		"pod":        `label_values(http_requests_total{datacenter=~"$datacenter"}, pod)`,
	}
	for _, v := range vars {
		if !v.Multi || !v.IncludeAll {
			t.Errorf("%s: multi=%v includeAll=%v", v.Name, v.Multi, v.IncludeAll)
		}
		if v.Query.Query != want[v.Name] {
			t.Errorf("%s: query %q, want %q", v.Name, v.Query.Query, want[v.Name])
		}
		if v.Datasource.UID != "prometheus" {
			t.Errorf("%s: datasource %q", v.Name, v.Datasource.UID)
		}
	}
}

func TestExpressions(t *testing.T) {
	h := Handler{Method: "GET", Route: "/items/{id}"}
	sel := `method="GET", route="/items/{id}", datacenter=~"$datacenter", pod=~"$pod"`

	tests := map[string]string{
		LatencyExpr(h, 0.99): `histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket{` + sel + `}[$__rate_interval])))`,
		RPSExpr(h):           `sum(rate(http_requests_total{` + sel + `}[$__rate_interval]))`,
		ErrorsExpr(h):        `sum by (code) (rate(http_requests_total{` + sel + `, code=~"4..|5.."}[$__rate_interval]))`,
	}
	for got, want := range tests {
		if got != want {
			t.Errorf("\ngot  %s\nwant %s", got, want)
		}
	}
}

func TestLatencyPanelHasAllQuantiles(t *testing.T) {
	latency := Build(testHandlers[:1], testOpts).Panels[1]

	var legends []string
	for _, tg := range latency.Targets {
		legends = append(legends, tg.LegendFormat)
	}
	if got := strings.Join(legends, ","); got != "p50,p95,p99" {
		t.Fatalf("legends %s", got)
	}
}

func TestSelectorEscapesQuotes(t *testing.T) {
	got := Selector(Handler{Method: "GET", Route: `/a"b`})
	if !strings.Contains(got, `route="/a\"b"`) {
		t.Fatalf("route not escaped: %s", got)
	}
}

func TestJSONShape(t *testing.T) {
	raw, err := json.Marshal(Build(testHandlers, testOpts))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"uid", "title", "editable", "panels", "templating", "time"} {
		if _, ok := m[key]; !ok {
			t.Errorf("missing key %q", key)
		}
	}
	if m["editable"] != false {
		t.Errorf("editable = %v", m["editable"])
	}
}
