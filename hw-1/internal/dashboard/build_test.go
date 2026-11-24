package dashboard

import (
	"encoding/json"
	"strings"
	"testing"
)

var handlers = []Handler{
	{Method: "POST", Route: "/items"},
	{Method: "GET", Route: "/items/{id}"},
	{Method: "PUT", Route: "/items/{id}"},
}

func TestBuild(t *testing.T) {
	d := Build(Options{UID: "u", Title: "t", DatasourceUID: "prometheus"}, handlers)

	if d.Editable {
		t.Error("dashboard must be non-editable")
	}
	if len(d.Panels) != len(handlers)*4 {
		t.Fatalf("panels: got %d", len(d.Panels))
	}

	ids := map[int]bool{}
	for _, p := range d.Panels {
		if ids[p.ID] {
			t.Errorf("duplicate panel id %d", p.ID)
		}
		ids[p.ID] = true
		for _, tg := range p.Targets {
			if tg.Datasource.UID != "prometheus" {
				t.Errorf("%s: datasource %q", p.Title, tg.Datasource.UID)
			}
		}
	}

	row, lat, rps, errs := d.Panels[4], d.Panels[5], d.Panels[6], d.Panels[7]
	if row.Type != "row" || row.Title != "GET /items/{id}" {
		t.Errorf("row: %+v", row)
	}
	sel := `method="GET", route="/items/{id}", datacenter=~"$datacenter", pod=~"$pod"`
	if len(lat.Targets) != 3 ||
		lat.Targets[2].Expr != `histogram_quantile(0.99, sum by (le) (rate(http_server_request_duration_seconds_bucket{`+sel+`}[$__rate_interval])))` {
		t.Errorf("latency: %+v", lat.Targets)
	}
	if rps.Targets[0].Expr != `sum(rate(http_server_requests_total{`+sel+`}[$__rate_interval]))` {
		t.Errorf("rps: %s", rps.Targets[0].Expr)
	}
	if lat.Options == nil || lat.Options.Tooltip.Mode != "multi" {
		t.Errorf("latency tooltip: %+v", lat.Options)
	}
	if !strings.HasPrefix(errs.Targets[0].Expr, "sum by (code) (") || !strings.Contains(errs.Targets[0].Expr, `code=~"4..|5.."`) {
		t.Errorf("errors: %s", errs.Targets[0].Expr)
	}
}

func TestVariables(t *testing.T) {
	d := Build(Options{DatasourceUID: "prometheus"}, nil)
	if len(d.Templating.List) != 2 {
		t.Fatalf("variables: %+v", d.Templating.List)
	}
	for _, v := range d.Templating.List {
		if !v.Multi || !v.IncludeAll {
			t.Errorf("%s: multi=%v all=%v", v.Name, v.Multi, v.IncludeAll)
		}
	}
	if !strings.Contains(d.Templating.List[1].Query, `datacenter=~"$datacenter"`) {
		t.Error("pod must depend on datacenter")
	}
	b, _ := json.Marshal(d)
	if !strings.Contains(string(b), `"editable":false`) {
		t.Error("editable must be serialized")
	}
}
