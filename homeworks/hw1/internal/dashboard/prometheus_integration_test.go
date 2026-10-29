//go:build integration

package dashboard

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"highload/lab1/pkg/monitoring"
)

// TestPrometheusEvaluatesGeneratedQueries runs the dashboard's own PromQL
// against Prometheus v3.15.1. The series are synthetic counters, not application
// traffic. Grafana macros are replaced the way the dashboard runtime does:
// $__rate_interval becomes the controlled window, and :regex values are escaped.
func TestPrometheusEvaluatesGeneratedQueries(t *testing.T) {
	bounds := monitoring.DurationBuckets()
	b1, b2, b3 := bounds[0], bounds[1], bounds[2]
	const (
		route  = "/widgets/{id}"
		dotted = "/dotted/{id}"
		window = "30s"
	)

	spec, err := Build([]Endpoint{{Method: "GET", Path: route}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	dottedSpec, err := Build([]Endpoint{{Method: "GET", Path: dotted}})
	if err != nil {
		t.Fatalf("build dotted: %v", err)
	}
	p50 := targetAt(t, panelAt(t, spec, 0), 0).Expr
	p95 := targetAt(t, panelAt(t, spec, 0), 1).Expr
	p99 := targetAt(t, panelAt(t, spec, 0), 2).Expr
	rps := targetAt(t, panelAt(t, spec, 1), 0).Expr
	errorsQuery := targetAt(t, panelAt(t, spec, 2), 0).Expr
	dottedRPS := targetAt(t, panelAt(t, dottedSpec, 1), 0).Expr
	for _, expr := range []string{p50, p95, p99} {
		if !strings.Contains(expr, "histogram_quantile(") || !strings.Contains(expr, "sum by (le)") {
			t.Fatalf("latency query is not an aggregated histogram quantile: %s", expr)
		}
	}
	if !strings.HasPrefix(rps, "sum(rate(") || strings.Contains(rps, " or ") {
		t.Fatalf("rps query = %s", rps)
	}
	if !strings.Contains(errorsQuery, "sum by (status_code)") || !strings.Contains(errorsQuery, `status_code=~"4..|5.."`) {
		t.Fatalf("error query = %s", errorsQuery)
	}

	start := time.Now()
	srv := &http.Server{
		Addr:              "0.0.0.0:0",
		ReadHeaderTimeout: 2 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			elapsed := time.Since(start).Seconds()
			switch r.URL.Path {
			case "/metrics":
				writeFixture(w, elapsed, b1, b2, b3, route, dotted)
			case "/decoy":
				writeDecoy(w, elapsed, route)
			default:
				http.NotFound(w, r)
			}
		}),
	}
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
	})

	cfgDir, err := os.MkdirTemp("/tmp", "lab1-prom-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(cfgDir) })
	cfgPath := filepath.Join(cfgDir, "prometheus.yml")
	cfg := fmt.Sprintf(`global:
  scrape_interval: 1s
  scrape_timeout: 800ms
scrape_configs:
  - job_name: items-app
    metrics_path: /metrics
    static_configs:
      - targets: ['host.docker.internal:%d']
  - job_name: other
    metrics_path: /decoy
    static_configs:
      - targets: ['host.docker.internal:%d']
`, port, port)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}

	name := fmt.Sprintf("lab1-prom-fixture-%d", os.Getpid())
	run := exec.Command("docker", "run", "--rm", "-d",
		"--name", name,
		"--add-host", "host.docker.internal:host-gateway",
		"-p", "127.0.0.1::9090",
		"-v", cfgPath+":/etc/prometheus/prometheus.yml:ro",
		"prom/prometheus:v3.15.1",
		"--config.file=/etc/prometheus/prometheus.yml",
		"--storage.tsdb.path=/prometheus",
		"--web.listen-address=0.0.0.0:9090",
	)
	out, err := run.CombinedOutput()
	if err != nil {
		t.Fatalf("start prometheus: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", name).Run()
	})
	portOut, err := exec.Command("docker", "port", name, "9090/tcp").Output()
	if err != nil {
		t.Fatalf("prometheus port: %v", err)
	}
	promAddr := strings.TrimSpace(string(portOut))
	base := "http://" + promAddr

	deadline := time.Now().Add(40 * time.Second)
	var ready bool
	var lastErr error
	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)
		targets, err := promQuery(base, "count(up == 1)")
		if err != nil {
			lastErr = err
			continue
		}
		if targets[""] >= 2 {
			ready = true
			break
		}
		lastErr = fmt.Errorf("healthy targets = %v", targets)
	}
	if !ready {
		logs, _ := exec.Command("docker", "logs", "--tail", "40", name).CombinedOutput()
		t.Fatalf("prometheus did not scrape both jobs: %v\n%s", lastErr, logs)
	}
	// Two scrapes are enough for rate(); wait until the window is inside the series.
	time.Sleep(35 * time.Second)

	all := choice{allDatacenters: true, allPods: true}
	both := choice{datacenters: []string{"dc-a", "dc-b"}, pods: []string{"pod-a", "pod-b"}}
	onlyA := choice{datacenters: []string{"dc-a"}, pods: []string{"pod-a"}}
	onlyB := choice{datacenters: []string{"dc-b"}, pods: []string{"pod-b"}}
	datacenterA := choice{datacenters: []string{"dc-a"}, allPods: true}
	podA := choice{allDatacenters: true, pods: []string{"pod-a"}}
	mismatch := choice{datacenters: []string{"dc-a"}, pods: []string{"pod-b"}}

	wantBothP50 := b1 + (b2-b1)/2
	wantBothP95 := b2 + 5*(b3-b2)/6
	wantBothP99 := b2 + 29*(b3-b2)/30
	wantAP50 := b1 + (b2-b1)/4
	wantBP50 := b1 + 7*(b2-b1)/12

	for _, selected := range []choice{all, both} {
		assertProm(t, base, p50, selected, window, "", wantBothP50)
		assertProm(t, base, p95, selected, window, "", wantBothP95)
		assertProm(t, base, p99, selected, window, "", wantBothP99)
		assertProm(t, base, rps, selected, window, "", 20)
		assertProm(t, base, errorsQuery, selected, window, "400", 0.4)
		assertProm(t, base, errorsQuery, selected, window, "404", 0.4)
		assertProm(t, base, errorsQuery, selected, window, "500", 0.2)
		assertPromAbsent(t, base, errorsQuery, selected, window, "200")
	}
	assertProm(t, base, rps, onlyA, window, "", 5)
	assertProm(t, base, rps, datacenterA, window, "", 5)
	assertProm(t, base, rps, podA, window, "", 5)
	assertProm(t, base, p50, onlyA, window, "", wantAP50)
	assertProm(t, base, p50, datacenterA, window, "", wantAP50)
	assertProm(t, base, p50, podA, window, "", wantAP50)
	assertProm(t, base, errorsQuery, onlyA, window, "400", 0.1)
	assertProm(t, base, errorsQuery, onlyA, window, "404", 0.1)
	assertProm(t, base, errorsQuery, onlyA, window, "500", 0.05)

	assertProm(t, base, rps, onlyB, window, "", 15)
	assertProm(t, base, p50, onlyB, window, "", wantBP50)
	assertProm(t, base, errorsQuery, onlyB, window, "400", 0.3)
	assertProm(t, base, errorsQuery, onlyB, window, "404", 0.3)
	assertProm(t, base, errorsQuery, onlyB, window, "500", 0.15)

	assertPromEmpty(t, base, p50, mismatch, window)
	assertPromEmpty(t, base, rps, mismatch, window)
	assertPromEmpty(t, base, errorsQuery, mismatch, window)

	gotBoth := promScalar(t, base, p50, all, window, "")
	gotA := promScalar(t, base, p50, onlyA, window, "")
	gotB := promScalar(t, base, p50, onlyB, window, "")
	averaged := (gotA + gotB) / 2
	if math.Abs(gotBoth-averaged) <= 1e-12 {
		t.Fatalf("aggregated p50 %g collapsed to the average of pod percentiles %g", gotBoth, averaged)
	}
	t.Logf("prometheus p50 all=%g podA=%g podB=%g average=%g", gotBoth, gotA, gotB, averaged)

	dottedChoice := choice{datacenters: []string{"dc.a"}, allPods: true}
	assertProm(t, base, dottedRPS, dottedChoice, window, "", 5)

	// Grafana's escapeRegex writes one backslash. The matcher is a PromQL raw
	// string, so that backslash reaches the regex engine and a dotted label matches.
	grafanaDot := strings.ReplaceAll(dottedRPS, "$__rate_interval", window)
	grafanaDot = strings.ReplaceAll(grafanaDot, "${datacenter:regex}", `dc\.a`)
	grafanaDot = strings.ReplaceAll(grafanaDot, "${pod_id:regex}", ".*")
	got, err := promQuery(base, grafanaDot)
	if err != nil {
		t.Fatalf("dotted datacenter through grafana escaping: %v\n%s", err, grafanaDot)
	}
	if math.Abs(got[""]-5) > 0.01 {
		t.Fatalf("dotted datacenter = %v, want 5\n%s", got, grafanaDot)
	}
}

func writeFixture(w http.ResponseWriter, elapsed, b1, b2, b3 float64, route, dotted string) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	type pod struct {
		dc, id          string
		b1, b2, b3, all float64
		statuses        map[string]float64
	}
	pods := []pod{
		{"dc-a", "pod-a", 2, 4, 5, 5, map[string]float64{"200": 4.75, "400": 0.1, "404": 0.1, "500": 0.05}},
		{"dc-b", "pod-b", 4, 10, 15, 15, map[string]float64{"200": 14.25, "400": 0.3, "404": 0.3, "500": 0.15}},
	}
	bounds := monitoring.DurationBuckets()
	for _, item := range pods {
		for _, bound := range bounds {
			rate := bucketRate(bound, b1, b2, b3, item.b1, item.b2, item.b3, item.all)
			fmt.Fprintf(w, "http_request_duration_seconds_bucket{%s,le=%q} %s\n",
				labels(item.dc, item.id, "GET", route), formatLE(bound), formatValue(rate*elapsed))
		}
		fmt.Fprintf(w, "http_request_duration_seconds_bucket{%s,le=\"+Inf\"} %s\n",
			labels(item.dc, item.id, "GET", route), formatValue(item.all*elapsed))
		for code, rate := range item.statuses {
			fmt.Fprintf(w, "http_requests_total{%s,status_code=%q} %s\n",
				labels(item.dc, item.id, "GET", route), code, formatValue(rate*elapsed))
		}
		fmt.Fprintf(w, "http_requests_total{%s,status_code=\"500\"} %s\n",
			labels(item.dc, item.id, "GET", "/other"), formatValue(1000*elapsed))
	}
	fmt.Fprintf(w, "http_requests_total{%s,status_code=\"200\"} %s\n",
		labels("dc.a", "pod-a", "GET", dotted), formatValue(5*elapsed))
	fmt.Fprintf(w, "http_requests_total{%s,status_code=\"200\"} %s\n",
		labels("dcXa", "pod-a", "GET", dotted), formatValue(100*elapsed))
}

func writeDecoy(w http.ResponseWriter, elapsed float64, route string) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "http_requests_total{%s,status_code=\"500\"} %s\n",
		labels("dc-a", "pod-a", "GET", route), formatValue(1000*elapsed))
}

func bucketRate(bound, b1, b2, b3, r1, r2, r3, total float64) float64 {
	switch {
	case bound < b1:
		return 0
	case bound < b2:
		return r1
	case bound < b3:
		return r2
	case bound == b3:
		return r3
	default:
		return total
	}
}

func labels(dc, pod, method, route string) string {
	return fmt.Sprintf("datacenter=%q,method=%q,pod_id=%q,route=%q", dc, method, pod, route)
}

func formatLE(bound float64) string {
	return strconv.FormatFloat(bound, 'f', -1, 64)
}

func formatValue(value float64) string {
	return strconv.FormatFloat(value, 'f', 8, 64)
}

func assertProm(t *testing.T, base, expr string, selected choice, window, group string, want float64) {
	t.Helper()
	got := promScalar(t, base, expr, selected, window, group)
	t.Logf("selection %+v group %q = %.12g want %.12g", selected, group, got, want)
	tol := math.Max(0.01, 0.02*math.Abs(want))
	if math.Abs(got-want) > tol {
		t.Fatalf("query selection %+v group %q = %g, want %g (tol %g)\n%s", selected, group, got, want, tol, applyChoice(expr, selected, window))
	}
}

func assertPromAbsent(t *testing.T, base, expr string, selected choice, window, group string) {
	t.Helper()
	values := promVector(t, base, expr, selected, window)
	if _, ok := values[group]; ok {
		t.Fatalf("group %s unexpectedly present in %v", group, values)
	}
}

func assertPromEmpty(t *testing.T, base, expr string, selected choice, window string) {
	t.Helper()
	values := promVector(t, base, expr, selected, window)
	if len(values) != 0 {
		t.Fatalf("selection %+v returned %v for %s", selected, values, applyChoice(expr, selected, window))
	}
}

func promScalar(t *testing.T, base, expr string, selected choice, window, group string) float64 {
	t.Helper()
	values := promVector(t, base, expr, selected, window)
	got, ok := values[group]
	if !ok {
		t.Fatalf("query selection %+v group %q = %v\n%s", selected, group, values, applyChoice(expr, selected, window))
	}
	return got
}

func promVector(t *testing.T, base, expr string, selected choice, window string) map[string]float64 {
	t.Helper()
	values, err := promQuery(base, applyChoice(expr, selected, window))
	if err != nil {
		t.Fatalf("prometheus: %v", err)
	}
	return values
}

func applyChoice(expr string, selected choice, window string) string {
	expr = strings.ReplaceAll(expr, "$__rate_interval", window)
	expr = strings.ReplaceAll(expr, "${datacenter:regex}", promqlRegex(selected.allDatacenters, selected.datacenters))
	return strings.ReplaceAll(expr, "${pod_id:regex}", promqlRegex(selected.allPods, selected.pods))
}

// promqlRegex is the text Grafana 13.3 inserts for ${var:regex}.
// Matchers are PromQL raw strings, so one backslash from escapeRegex stays a
// regex escape. A saved All value is the raw allValue ".*": the custom-all
// formatter does not run escapeRegex on it. Several concrete values become "(a|b)".
func promqlRegex(all bool, values []string) string {
	if all {
		return ".*"
	}
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = regexp.QuoteMeta(value)
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return "(" + strings.Join(parts, "|") + ")"
}

func promQuery(base, expr string) (map[string]float64, error) {
	target := base + "/api/v1/query?query=" + url.QueryEscape(expr)
	resp, err := http.Get(target)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	var payload struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	if payload.Status != "success" {
		return nil, fmt.Errorf("%s", payload.Error)
	}
	out := map[string]float64{}
	for _, sample := range payload.Data.Result {
		if len(sample.Value) != 2 {
			return nil, fmt.Errorf("sample %#v", sample.Value)
		}
		text, ok := sample.Value[1].(string)
		if !ok {
			return nil, fmt.Errorf("value %#v", sample.Value[1])
		}
		value, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, err
		}
		group := sample.Metric["status_code"]
		out[group] = value
	}
	return out, nil
}
