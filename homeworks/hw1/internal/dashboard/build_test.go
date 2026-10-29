package dashboard

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/grafana/grafana-foundation-sdk/go/common"
	gf "github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/prometheus"
)

func TestBuildPanelsFromCatalog(t *testing.T) {
	t.Parallel()

	handlers := []Endpoint{
		{Method: "POST", Path: "/widgets"},
		{Method: "GET", Path: "/widgets/{id}"},
		{Method: "PUT", Path: "/widgets/{id}"},
	}
	spec, err := Build(handlers)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if deref(spec.Uid) != dashboardUID || spec.Editable == nil || *spec.Editable || deref(spec.Refresh) != "5s" || spec.SchemaVersion != schemaVersion {
		t.Fatalf("dashboard header uid=%s editable=%v refresh=%s schema=%d", deref(spec.Uid), spec.Editable, deref(spec.Refresh), spec.SchemaVersion)
	}
	if spec.Time == nil || spec.Time.From != "now-15m" || spec.Time.To != "now" {
		t.Fatalf("time = %+v", spec.Time)
	}
	if len(spec.Panels) != len(handlers)*3 {
		t.Fatalf("panels = %d", len(spec.Panels))
	}
	for i, handler := range handlers {
		latency := panelAt(t, spec, i*3)
		rps := panelAt(t, spec, i*3+1)
		errorsPanel := panelAt(t, spec, i*3+2)
		prefix := handler.Method + " " + handler.Path
		if deref(latency.Title) != prefix+" latency" || deref(rps.Title) != prefix+" RPS" || deref(errorsPanel.Title) != prefix+" errors" {
			t.Fatalf("titles = %s, %s, %s", deref(latency.Title), deref(rps.Title), deref(errorsPanel.Title))
		}
		if unitOf(latency) != "s" || unitOf(rps) != "reqps" || unitOf(errorsPanel) != "reqps" {
			t.Fatal("units are wrong")
		}
		if len(latency.Targets) != 3 || deref(targetAt(t, latency, 0).LegendFormat) != "p50" || deref(targetAt(t, latency, 1).LegendFormat) != "p95" || deref(targetAt(t, latency, 2).LegendFormat) != "p99" {
			t.Fatalf("latency targets = %+v", latency.Targets)
		}
		if deref(targetAt(t, errorsPanel, 0).LegendFormat) != "{{status_code}}" {
			t.Fatalf("error legend = %s", deref(targetAt(t, errorsPanel, 0).LegendFormat))
		}
		for _, item := range []gf.Panel{latency, rps, errorsPanel} {
			if item.Datasource == nil || deref(item.Datasource.Uid) != datasourceUID || item.Type != "timeseries" {
				t.Fatalf("panel datasource = %+v", item.Datasource)
			}
			if spansNulls(item) {
				t.Fatal("nulls are spanned")
			}
			for index := range item.Targets {
				target := targetAt(t, item, index)
				if target.Datasource == nil || deref(target.Datasource.Uid) != datasourceUID {
					t.Fatalf("target datasource = %+v", target.Datasource)
				}
				if strings.Contains(target.Expr, "vector(0)") || strings.Contains(target.Expr, " or ") {
					t.Fatalf("query replaces missing data: %s", target.Expr)
				}
			}
		}
	}

	replaced, err := Build(handlers[:2])
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	for _, expr := range expressionsOf(replaced) {
		if strings.Contains(expr, `route="/widgets/{id}",`) && strings.Contains(expr, `method="PUT"`) {
			t.Fatalf("removed route is still present: %s", expr)
		}
	}
	if len(replaced.Panels) != 6 {
		t.Fatalf("replaced panels = %d", len(replaced.Panels))
	}
}

func TestBuildRejectsRoutesTheRegistrarWould(t *testing.T) {
	t.Parallel()

	if _, err := Build([]Endpoint{{Method: "GET", Path: `/v1/"id"`}}); err == nil {
		t.Fatal("quoted path was accepted")
	}
}

func TestBuildRoundTripsRouteThroughJSON(t *testing.T) {
	t.Parallel()

	path := "/widgets/{id}"
	spec, err := Build([]Endpoint{{Method: "GET", Path: path}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	expr := targetAt(t, panelAt(t, spec, 0), 0).Expr
	if !strings.Contains(expr, `route="/widgets/{id}"`) {
		t.Fatalf("promql literal = %s", expr)
	}
	payload, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Spec
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if targetAt(t, panelAt(t, decoded, 0), 0).Expr != expr {
		t.Fatalf("round trip expr = %s", targetAt(t, panelAt(t, decoded, 0), 0).Expr)
	}
}

func TestVariablesKeepAllWhenMetricsAreEmpty(t *testing.T) {
	t.Parallel()

	spec, err := Build([]Endpoint{{Method: "GET", Path: "/widgets/{id}"}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(spec.Templating.List) != 2 {
		t.Fatalf("variables = %+v", spec.Templating.List)
	}
	datacenter := spec.Templating.List[0]
	pod := spec.Templating.List[1]
	for _, item := range []gf.VariableModel{datacenter, pod} {
		if !boolValue(item.Multi) || !boolValue(item.IncludeAll) || deref(item.AllValue) != allRegexValue {
			t.Fatalf("%s selection = %+v", item.Name, item)
		}
		if item.Current == nil || optionString(item.Current.Value) != allVariableValue || optionString(item.Current.Text) != "All" {
			t.Fatalf("%s current = %+v", item.Name, item.Current)
		}
		if item.Refresh == nil || *item.Refresh != gf.VariableRefreshOnTimeRangeChanged {
			t.Fatalf("%s refresh = %v", item.Name, item.Refresh)
		}
		if item.Datasource == nil || deref(item.Datasource.Uid) != datasourceUID {
			t.Fatalf("%s datasource = %+v", item.Name, item.Datasource)
		}
		if len(item.Options) != 1 || optionString(item.Options[0].Value) != allVariableValue {
			t.Fatalf("%s options pin a concrete list: %+v", item.Name, item.Options)
		}
		if queryText(item.Query) != deref(item.Definition) {
			t.Fatalf("%s query = %s definition = %s", item.Name, queryText(item.Query), deref(item.Definition))
		}
	}
	if datacenter.Name != "datacenter" || !strings.Contains(queryText(datacenter.Query), `job="items-app"`) {
		t.Fatalf("datacenter query = %s", queryText(datacenter.Query))
	}
	if pod.Name != "pod_id" || !strings.Contains(queryText(pod.Query), "datacenter=~`${datacenter:regex}`") || !strings.Contains(queryText(pod.Query), "pod_id") {
		t.Fatalf("pod query = %s", queryText(pod.Query))
	}
}

func panelAt(t *testing.T, spec Spec, index int) gf.Panel {
	t.Helper()
	if index < 0 || index >= len(spec.Panels) || spec.Panels[index].Panel == nil {
		t.Fatalf("panel %d is missing", index)
	}
	return *spec.Panels[index].Panel
}

func targetAt(t *testing.T, panel gf.Panel, index int) prometheus.Dataquery {
	t.Helper()
	if index < 0 || index >= len(panel.Targets) {
		t.Fatalf("target %d is missing", index)
	}
	switch query := panel.Targets[index].(type) {
	case prometheus.Dataquery:
		return query
	case *prometheus.Dataquery:
		return *query
	default:
		t.Fatalf("target %d has type %T", index, panel.Targets[index])
		return prometheus.Dataquery{}
	}
}

func unitOf(panel gf.Panel) string {
	if panel.FieldConfig == nil {
		return ""
	}
	return deref(panel.FieldConfig.Defaults.Unit)
}

func spansNulls(panel gf.Panel) bool {
	if panel.FieldConfig == nil || panel.FieldConfig.Defaults.Custom == nil {
		return false
	}
	custom, ok := panel.FieldConfig.Defaults.Custom.(*common.GraphFieldConfig)
	if !ok || custom.SpanNulls == nil || custom.SpanNulls.Bool == nil {
		return true
	}
	return *custom.SpanNulls.Bool
}

func TestPromQLStringLiteral(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		`plain`:       `"plain"`,
		`/items/{id}`: `"/items/{id}"`,
		`a"b`:         `"a\"b"`,
		`a\b`:         `"a\\b"`,
		"a\nb":        `"a\nb"`,
	}
	for value, want := range tests {
		if got := promqlString(value); got != want {
			t.Fatalf("promqlString(%q) = %s, want %s", value, got, want)
		}
	}
}

func TestBusinessCatalogShapeHasNinePanels(t *testing.T) {
	t.Parallel()

	spec, err := Build([]Endpoint{
		{Method: "POST", Path: "/items"},
		{Method: "GET", Path: "/items/{id}"},
		{Method: "PUT", Path: "/items/{id}"},
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(spec.Panels) != 9 {
		t.Fatalf("panels = %d", len(spec.Panels))
	}
}

func TestQueriesUseSharedMetricNames(t *testing.T) {
	t.Parallel()

	latency := latencyQuery("GET", "/widgets/{id}", p95)
	if latency.metric != metricDuration || latency.quantile != p95 || !strings.Contains(latency.form, "sum by (le)") {
		t.Fatalf("latency = %+v", latency)
	}
	expr := latency.String()
	if !strings.Contains(expr, "datacenter=~`${datacenter:regex}`") || !strings.Contains(expr, `route="/widgets/{id}"`) {
		t.Fatalf("latency expr = %s", expr)
	}
	rps := rpsQuery("GET", "/widgets/{id}")
	if rps.metric != metricRequests || !strings.Contains(rps.form, "sum(rate(") {
		t.Fatalf("rps = %+v", rps)
	}
	got := errorQuery("GET", "/widgets/{id}").String()
	if !strings.Contains(got, metricRequests) || !strings.Contains(got, `status_code=~"4..|5.."`) || !strings.Contains(got, "sum by (status_code)") {
		t.Fatalf("errors = %s", got)
	}
}
