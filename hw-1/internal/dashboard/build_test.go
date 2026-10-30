package dashboard

import (
	"strconv"
	"strings"
	"testing"
)

func TestBuildUsesDiscoveredEndpoints(t *testing.T) {
	endpoints := []Endpoint{{Method: "PATCH", Path: "/widgets/{key}"}, {Method: "GET", Path: "/other"}}
	document := Build(endpoints)
	panels := document["panels"].([]Object)
	if len(panels) != 6 || document["editable"] != false {
		t.Fatal("неверное число панелей или разрешено редактирование")
	}
	for i, endpoint := range endpoints {
		for _, p := range panels[i*3 : i*3+3] {
			for _, query := range p["targets"].([]Object) {
				expression := query["expr"].(string)
				for _, required := range []string{"method=" + strconv.Quote(endpoint.Method), "route=" + strconv.Quote(endpoint.Path),
					`datacenter=~"${datacenter:regex}"`, `pod_id=~"${pod_id:regex}"`} {
					if !strings.Contains(expression, required) {
						t.Errorf("в запросе нет %s", required)
					}
				}
			}
		}
	}
	variables := document["templating"].(Object)["list"].([]Object)
	for _, variable := range variables {
		if variable["multi"] != true || variable["includeAll"] != true {
			t.Error("нет выбора нескольких значений и All")
		}
	}
}

func TestQueriesAggregateBucketsAndCounters(t *testing.T) {
	panels := Build([]Endpoint{{Method: "GET", Path: "/example"}})["panels"].([]Object)
	latency := panels[0]["targets"].([]Object)
	if len(latency) != 3 {
		t.Fatal("ожидались три перцентиля")
	}
	for i, q := range []string{"0.5", "0.95", "0.99"} {
		expr := latency[i]["expr"].(string)
		if !strings.HasPrefix(expr, "histogram_quantile("+q+", sum by (le) (rate(") {
			t.Errorf("неверная агрегация: %s", expr)
		}
	}
	rps := panels[1]["targets"].([]Object)[0]["expr"].(string)
	if !strings.HasPrefix(rps, "sum(rate(") {
		t.Errorf("неверный RPS: %s", rps)
	}
	errors := panels[2]["targets"].([]Object)[0]["expr"].(string)
	if !strings.Contains(errors, `status_code=~"4..|5.."`) || !strings.HasPrefix(errors, "sum by (status_code)") {
		t.Errorf("нет разбивки ошибок по кодам: %s", errors)
	}
}

func TestRouteIsQuotedForPromQL(t *testing.T) {
	path := "/odd/\"quoted\\path"
	panels := Build([]Endpoint{{Method: "GET", Path: path}})["panels"].([]Object)
	expr := panels[1]["targets"].([]Object)[0]["expr"].(string)
	if !strings.Contains(expr, "route="+strconv.Quote(path)) {
		t.Fatal("путь не экранирован")
	}
}
