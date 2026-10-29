package dashboard

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"highload/lab1/pkg/monitoring"
)

func TestSelectedSeriesAggregation(t *testing.T) {
	t.Parallel()

	bounds := monitoring.DurationBuckets()
	b1, b2, b3 := bounds[0], bounds[1], bounds[2]
	route := "/widgets/{id}"
	spec, err := Build([]Endpoint{{Method: "GET", Path: route}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	p50 := targetAt(t, panelAt(t, spec, 0), 0).Expr
	p95 := targetAt(t, panelAt(t, spec, 0), 1).Expr
	p99 := targetAt(t, panelAt(t, spec, 0), 2).Expr
	rps := targetAt(t, panelAt(t, spec, 1), 0).Expr
	errorsQuery := targetAt(t, panelAt(t, spec, 2), 0).Expr

	samples := aggregationFixture(b1, b2, b3, route)
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
		assertScalar(t, p50, samples, selected, wantBothP50)
		assertScalar(t, p95, samples, selected, wantBothP95)
		assertScalar(t, p99, samples, selected, wantBothP99)
		assertScalar(t, rps, samples, selected, 20)
		assertStatuses(t, errorsQuery, samples, selected, map[string]float64{"400": 0.4, "404": 0.4, "500": 0.2})
	}
	assertScalar(t, p50, samples, onlyA, wantAP50)
	assertScalar(t, p50, samples, datacenterA, wantAP50)
	assertScalar(t, p50, samples, podA, wantAP50)
	assertScalar(t, rps, samples, onlyA, 5)
	assertScalar(t, rps, samples, datacenterA, 5)
	assertScalar(t, rps, samples, podA, 5)
	assertStatuses(t, errorsQuery, samples, onlyA, map[string]float64{"400": 0.1, "404": 0.1, "500": 0.05})

	assertScalar(t, p50, samples, onlyB, wantBP50)
	assertScalar(t, rps, samples, onlyB, 15)
	assertStatuses(t, errorsQuery, samples, onlyB, map[string]float64{"400": 0.3, "404": 0.3, "500": 0.15})

	assertAbsent(t, p50, samples, mismatch)
	assertAbsent(t, rps, samples, mismatch)
	assertAbsent(t, errorsQuery, samples, mismatch)

	averaged := (wantAP50 + wantBP50) / 2
	if math.Abs(averaged-wantBothP50) <= 1e-12 {
		t.Fatal("fixture does not distinguish averaged percentiles from aggregated buckets")
	}
	if math.Abs(scalar(t, p50, samples, all)-averaged) <= 1e-12 {
		t.Fatal("aggregated p50 collapsed to the average of pod percentiles")
	}
}

func TestRegexFilterDoesNotTreatDotAsAnyCharacter(t *testing.T) {
	t.Parallel()

	route := "/widgets/{id}"
	spec, err := Build([]Endpoint{{Method: "GET", Path: route}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	rps := targetAt(t, panelAt(t, spec, 1), 0).Expr
	samples := []sample{
		counterSample("dc.a", "pod-a", route, "200", 5),
		counterSample("dcXa", "pod-a", route, "200", 100),
	}
	assertScalar(t, rps, samples, choice{datacenters: []string{"dc.a"}, allPods: true}, 5)
}

func TestEvaluatorRejectsCollapsedPercentiles(t *testing.T) {
	t.Parallel()

	rejected := []string{
		`avg(histogram_quantile(0.95, rate(http_request_duration_seconds_bucket{job="items-app"}[5s])))`,
		`histogram_quantile(0.95, avg by (le) (rate(http_request_duration_seconds_bucket{job="items-app"}[5s])))`,
		`sum(rate(http_requests_total{job="items-app"}[5s])) or vector(0)`,
	}
	for _, expr := range rejected {
		if _, err := evaluate(expr, nil); err == nil {
			t.Fatalf("accepted a query that does not aggregate buckets before the percentile: %s", expr)
		}
	}
}

func TestGeneratorSourceHasNoRouteList(t *testing.T) {
	t.Parallel()

	roots := []string{".", filepath.Join("..", "..", "cmd", "dashboard-generator")}
	for _, root := range roots {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if strings.Contains(string(body), "/items") {
				return fmt.Errorf("%s contains a built-in application route", path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

type choice struct {
	allDatacenters bool
	datacenters    []string
	allPods        bool
	pods           []string
}

type sample struct {
	labels map[string]string
	value  float64
}

func aggregationFixture(b1, b2, b3 float64, route string) []sample {
	samples := []sample{}
	appendHistogram(&samples, "dc-a", "pod-a", route, map[float64]float64{
		b1: 2, b2: 4, b3: 5, math.Inf(1): 5,
	})
	appendHistogram(&samples, "dc-b", "pod-b", route, map[float64]float64{
		b1: 4, b2: 10, b3: 15, math.Inf(1): 15,
	})
	for _, status := range []struct {
		dc, pod, code string
		rate          float64
	}{
		{"dc-a", "pod-a", "200", 4.75},
		{"dc-a", "pod-a", "400", 0.1},
		{"dc-a", "pod-a", "404", 0.1},
		{"dc-a", "pod-a", "500", 0.05},
		{"dc-b", "pod-b", "200", 14.25},
		{"dc-b", "pod-b", "400", 0.3},
		{"dc-b", "pod-b", "404", 0.3},
		{"dc-b", "pod-b", "500", 0.15},
	} {
		samples = append(samples, counterSample(status.dc, status.pod, route, status.code, status.rate))
	}
	samples = append(samples,
		counterSample("dc-a", "pod-a", "/other", "500", 1000),
		sample{labels: map[string]string{
			"__name__": metricRequests, "job": "other", "method": "GET", "route": route,
			"datacenter": "dc-a", "pod_id": "pod-a", "status_code": "500",
		}, value: 1000},
	)
	return samples
}

func appendHistogram(samples *[]sample, dc, pod, route string, rates map[float64]float64) {
	for bound, rate := range rates {
		*samples = append(*samples, sample{labels: map[string]string{
			"__name__": metricDuration, "job": prometheusJob, "method": "GET", "route": route,
			"datacenter": dc, "pod_id": pod, "le": leLabel(bound),
		}, value: rate})
	}
}

func counterSample(dc, pod, route, status string, rate float64) sample {
	return sample{labels: map[string]string{
		"__name__": metricRequests, "job": prometheusJob, "method": "GET", "route": route,
		"datacenter": dc, "pod_id": pod, "status_code": status,
	}, value: rate}
}

func leLabel(bound float64) string {
	if math.IsInf(bound, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(bound, 'f', -1, 64)
}

func assertScalar(t *testing.T, expr string, samples []sample, selected choice, want float64) {
	t.Helper()
	got := scalar(t, expr, samples, selected)
	if math.Abs(got-want) > 1e-9 && math.Abs(got-want) > 1e-9*math.Abs(want) {
		t.Fatalf("query %s selection %+v = %g, want %g", expr, selected, got, want)
	}
}

func scalar(t *testing.T, expr string, samples []sample, selected choice) float64 {
	t.Helper()
	values := evaluateSelection(t, expr, samples, selected)
	if len(values) != 1 {
		t.Fatalf("query %s selection %+v = %v", expr, selected, values)
	}
	for _, value := range values {
		return value
	}
	return 0
}

func assertStatuses(t *testing.T, expr string, samples []sample, selected choice, want map[string]float64) {
	t.Helper()
	got := evaluateSelection(t, expr, samples, selected)
	if len(got) != len(want) {
		t.Fatalf("statuses = %v, want %v", got, want)
	}
	for code, wantValue := range want {
		gotValue, ok := got[code]
		if !ok || math.Abs(gotValue-wantValue) > 1e-9 {
			t.Fatalf("status %s = %v, want %v in %v", code, gotValue, wantValue, got)
		}
	}
	if _, ok := got["200"]; ok {
		t.Fatal("error panel includes status 200")
	}
}

func assertAbsent(t *testing.T, expr string, samples []sample, selected choice) {
	t.Helper()
	got := evaluateSelection(t, expr, samples, selected)
	if len(got) != 0 {
		t.Fatalf("selection %+v returned %v", selected, got)
	}
}

func evaluateSelection(t *testing.T, expr string, samples []sample, selected choice) map[string]float64 {
	t.Helper()
	values, err := evaluate(interpolate(expr, selected), samples)
	if err != nil {
		t.Fatalf("evaluate %s: %v", expr, err)
	}
	return values
}

func interpolate(expr string, selected choice) string {
	expr = strings.ReplaceAll(expr, "${datacenter:regex}", regexValue(selected.allDatacenters, selected.datacenters))
	return strings.ReplaceAll(expr, "${pod_id:regex}", regexValue(selected.allPods, selected.pods))
}

func regexValue(all bool, values []string) string {
	if all {
		return ".*"
	}
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = regexp.QuoteMeta(value)
	}
	return strings.Join(parts, "|")
}

type matcher struct {
	name  string
	value string
	regex bool
}

func evaluate(expr string, samples []sample) (map[string]float64, error) {
	expr = strings.TrimSpace(expr)
	switch {
	case strings.HasPrefix(expr, "histogram_quantile("):
		return evaluateLatency(expr, samples)
	case strings.HasPrefix(expr, "sum by (status_code) (rate("):
		return evaluateGrouped(expr, samples, "status_code")
	case strings.HasPrefix(expr, "sum(rate("):
		return evaluateSum(expr, samples)
	default:
		return nil, fmt.Errorf("query is not an aggregated rate or histogram quantile")
	}
}

func evaluateLatency(expr string, samples []sample) (map[string]float64, error) {
	rest := strings.TrimPrefix(expr, "histogram_quantile(")
	quantile, rest, err := readNumber(rest)
	if err != nil {
		return nil, err
	}
	rest, err = trimLiteral(rest, ", sum by (le) (rate(")
	if err != nil {
		return nil, err
	}
	metric, matchers, rest, err := readRateCall(rest)
	if err != nil {
		return nil, err
	}
	if metric != metricDuration {
		return nil, fmt.Errorf("latency metric is %s", metric)
	}
	if err := finish(rest, ")))"); err != nil {
		return nil, err
	}
	buckets := sumBuckets(filter(samples, metricDuration, matchers))
	value := histogramQuantile(quantile, buckets)
	if math.IsNaN(value) {
		return map[string]float64{}, nil
	}
	return map[string]float64{"": value}, nil
}

func evaluateSum(expr string, samples []sample) (map[string]float64, error) {
	rest, err := trimLiteral(expr, "sum(rate(")
	if err != nil {
		return nil, err
	}
	metric, matchers, rest, err := readRateCall(rest)
	if err != nil {
		return nil, err
	}
	if err := finish(rest, "))"); err != nil {
		return nil, err
	}
	matched := filter(samples, metric, matchers)
	if len(matched) == 0 {
		return map[string]float64{}, nil
	}
	var sum float64
	for _, item := range matched {
		sum += item.value
	}
	return map[string]float64{"": sum}, nil
}

func evaluateGrouped(expr string, samples []sample, group string) (map[string]float64, error) {
	rest, err := trimLiteral(expr, "sum by ("+group+") (rate(")
	if err != nil {
		return nil, err
	}
	metric, matchers, rest, err := readRateCall(rest)
	if err != nil {
		return nil, err
	}
	if err := finish(rest, "))"); err != nil {
		return nil, err
	}
	totals := map[string]float64{}
	for _, item := range filter(samples, metric, matchers) {
		totals[item.labels[group]] += item.value
	}
	return totals, nil
}

func readRateCall(expr string) (string, []matcher, string, error) {
	brace := strings.IndexByte(expr, '{')
	if brace <= 0 {
		return "", nil, "", fmt.Errorf("metric selector is missing")
	}
	metric := expr[:brace]
	matchers, rest, err := readSelector(expr[brace:])
	if err != nil {
		return "", nil, "", err
	}
	if !strings.HasPrefix(rest, "[") {
		return "", nil, "", fmt.Errorf("range window is missing")
	}
	end := strings.IndexByte(rest, ']')
	if end <= 1 {
		return "", nil, "", fmt.Errorf("range window is invalid")
	}
	return metric, matchers, rest[end+1:], nil
}

func readSelector(expr string) ([]matcher, string, error) {
	if !strings.HasPrefix(expr, "{") {
		return nil, "", fmt.Errorf("selector is missing")
	}
	rest := expr[1:]
	var matchers []matcher
	for {
		rest = strings.TrimLeft(rest, " ")
		if strings.HasPrefix(rest, "}") {
			return matchers, rest[1:], nil
		}
		name, next, err := readIdent(rest)
		if err != nil {
			return nil, "", err
		}
		regex := false
		switch {
		case strings.HasPrefix(next, "=~"):
			regex = true
			next = next[2:]
		case strings.HasPrefix(next, "="):
			next = next[1:]
		default:
			return nil, "", fmt.Errorf("matcher %s has no operator", name)
		}
		value, next, err := readQuoted(next)
		if err != nil {
			return nil, "", err
		}
		matchers = append(matchers, matcher{name: name, value: value, regex: regex})
		rest = strings.TrimLeft(next, " ")
		if strings.HasPrefix(rest, ",") {
			rest = rest[1:]
			continue
		}
		if strings.HasPrefix(rest, "}") {
			return matchers, rest[1:], nil
		}
		return nil, "", fmt.Errorf("selector did not end")
	}
}

func readIdent(expr string) (string, string, error) {
	i := 0
	for i < len(expr) && (expr[i] == '_' || (expr[i] >= 'a' && expr[i] <= 'z') || (expr[i] >= 'A' && expr[i] <= 'Z') || (i > 0 && expr[i] >= '0' && expr[i] <= '9')) {
		i++
	}
	if i == 0 {
		return "", "", fmt.Errorf("identifier is missing")
	}
	return expr[:i], expr[i:], nil
}

func readQuoted(expr string) (string, string, error) {
	if expr == "" {
		return "", "", fmt.Errorf("string is missing")
	}
	if expr[0] == '`' {
		end := strings.IndexByte(expr[1:], '`')
		if end < 0 {
			return "", "", fmt.Errorf("string is unterminated")
		}
		return expr[1 : 1+end], expr[2+end:], nil
	}
	if !strings.HasPrefix(expr, `"`) {
		return "", "", fmt.Errorf("string is missing")
	}
	var b strings.Builder
	for i := 1; i < len(expr); i++ {
		switch expr[i] {
		case '"':
			return b.String(), expr[i+1:], nil
		case '\\':
			if i+1 >= len(expr) {
				return "", "", fmt.Errorf("escape is truncated")
			}
			i++
			switch expr[i] {
			case '\\', '"':
				b.WriteByte(expr[i])
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			default:
				b.WriteByte('\\')
				b.WriteByte(expr[i])
			}
		default:
			b.WriteByte(expr[i])
		}
	}
	return "", "", fmt.Errorf("string is unterminated")
}

func readNumber(expr string) (float64, string, error) {
	i := 0
	for i < len(expr) && (expr[i] == '.' || expr[i] == '-' || (expr[i] >= '0' && expr[i] <= '9')) {
		i++
	}
	value, err := strconv.ParseFloat(expr[:i], 64)
	if err != nil {
		return 0, "", err
	}
	return value, expr[i:], nil
}

func trimLiteral(expr, prefix string) (string, error) {
	if !strings.HasPrefix(expr, prefix) {
		return "", fmt.Errorf("want %q", prefix)
	}
	return expr[len(prefix):], nil
}

func finish(expr, suffix string) error {
	rest, err := trimLiteral(expr, suffix)
	if err != nil {
		return err
	}
	if rest != "" {
		return fmt.Errorf("trailing %q", rest)
	}
	return nil
}

func filter(samples []sample, metric string, matchers []matcher) []sample {
	var out []sample
	for _, item := range samples {
		if item.labels["__name__"] != metric || !matches(item.labels, matchers) {
			continue
		}
		out = append(out, item)
	}
	return out
}

func matches(labels map[string]string, matchers []matcher) bool {
	for _, item := range matchers {
		got := labels[item.name]
		if item.regex {
			re, err := regexp.Compile("^(?:" + item.value + ")$")
			if err != nil || !re.MatchString(got) {
				return false
			}
			continue
		}
		if got != item.value {
			return false
		}
	}
	return true
}

type quantileBucket struct {
	upper float64
	count float64
}

func sumBuckets(samples []sample) []quantileBucket {
	totals := map[float64]float64{}
	for _, item := range samples {
		upper, err := strconv.ParseFloat(item.labels["le"], 64)
		if err != nil {
			continue
		}
		totals[upper] += item.value
	}
	bounds := make([]float64, 0, len(totals))
	for upper := range totals {
		bounds = append(bounds, upper)
	}
	sortFloats(bounds)
	out := make([]quantileBucket, 0, len(bounds))
	for _, upper := range bounds {
		out = append(out, quantileBucket{upper: upper, count: totals[upper]})
	}
	for i := 1; i < len(out); i++ {
		if out[i].count < out[i-1].count {
			out[i].count = out[i-1].count
		}
	}
	return out
}

func sortFloats(values []float64) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

func histogramQuantile(q float64, buckets []quantileBucket) float64 {
	if math.IsNaN(q) || len(buckets) < 2 || !math.IsInf(buckets[len(buckets)-1].upper, 1) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(1)
	}
	total := buckets[len(buckets)-1].count
	if total == 0 {
		return math.NaN()
	}
	rank := q * total
	index := len(buckets) - 1
	for i := 0; i < len(buckets)-1; i++ {
		if buckets[i].count >= rank {
			index = i
			break
		}
	}
	if index == len(buckets)-1 {
		return buckets[len(buckets)-2].upper
	}
	if index == 0 && buckets[0].upper <= 0 {
		return buckets[0].upper
	}
	start := 0.0
	count := buckets[index].count
	if index > 0 {
		start = buckets[index-1].upper
		count -= buckets[index-1].count
		rank -= buckets[index-1].count
	}
	if count == 0 {
		return math.NaN()
	}
	return start + (buckets[index].upper-start)*(rank/count)
}
