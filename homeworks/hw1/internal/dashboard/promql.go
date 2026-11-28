package dashboard

import (
	"strconv"
	"strings"

	"highload/lab1/pkg/monitoring"
)

const (
	prometheusJob = "items-app"
	rateWindow    = "$__rate_interval"
	errorStatuses = "4..|5.."

	p50 = 0.5
	p95 = 0.95
	p99 = 0.99

	datacenterVar = "${datacenter:regex}"
	podVar        = "${pod_id:regex}"

	// metricRequests and metricDuration are the series the dashboard reads.
	// The names themselves live in pkg/monitoring.
	metricRequests = monitoring.RequestsTotal
	metricDuration = monitoring.RequestDurationBucket

	latencyForm = "histogram_quantile({{q}}, sum by (le) (rate({{metric}}{{selector}}[{{window}}])))"
	rpsForm     = "sum(rate({{metric}}{{selector}}[{{window}}]))"
	errorForm   = "sum by (status_code) (rate({{metric}}{{selector}}[{{window}}]))"
	labelsForm  = "label_values({{metric}}{{selector}}, {{label}})"
)

// labelMatcher is one PromQL label match.
// raw values are Grafana placeholders written as PromQL raw strings.
// A raw string keeps the single backslash Grafana's :regex format inserts for '.',
// which Prometheus 3 rejects inside a double-quoted string.
type labelMatcher struct {
	label string
	op    string
	value string
	raw   bool
}

// query is one dashboard expression. String is the only place the text is assembled.
type query struct {
	form     string
	quantile float64
	metric   string
	label    string
	matchers []labelMatcher
}

func (q query) String() string {
	return strings.NewReplacer(
		"{{q}}", strconv.FormatFloat(q.quantile, 'f', -1, 64),
		"{{metric}}", q.metric,
		"{{selector}}", "{"+renderMatchers(q.matchers)+"}",
		"{{window}}", rateWindow,
		"{{label}}", q.label,
	).Replace(q.form)
}

func renderMatchers(matchers []labelMatcher) string {
	parts := make([]string, len(matchers))
	for i, matcher := range matchers {
		parts[i] = matcher.render()
	}
	return strings.Join(parts, ",")
}

func (m labelMatcher) render() string {
	if m.raw {
		return m.label + m.op + "`" + m.value + "`"
	}
	return m.label + m.op + promqlString(m.value)
}

func routeMatchers(method, route string) []labelMatcher {
	return []labelMatcher{
		{label: "job", op: "=", value: prometheusJob},
		{label: "method", op: "=", value: method},
		{label: "route", op: "=", value: route},
		{label: "datacenter", op: "=~", value: datacenterVar, raw: true},
		{label: "pod_id", op: "=~", value: podVar, raw: true},
	}
}

func latencyQuery(method, route string, quantile float64) query {
	return query{
		form:     latencyForm,
		quantile: quantile,
		metric:   metricDuration,
		matchers: routeMatchers(method, route),
	}
}

func rpsQuery(method, route string) query {
	return query{
		form:     rpsForm,
		metric:   metricRequests,
		matchers: routeMatchers(method, route),
	}
}

func errorQuery(method, route string) query {
	matchers := append(routeMatchers(method, route), labelMatcher{
		label: "status_code", op: "=~", value: errorStatuses,
	})
	return query{
		form:     errorForm,
		metric:   metricRequests,
		matchers: matchers,
	}
}

func datacenterVariableQuery() string {
	return (query{
		form:   labelsForm,
		metric: metricRequests,
		label:  "datacenter",
		matchers: []labelMatcher{
			{label: "job", op: "=", value: prometheusJob},
		},
	}).String()
}

func podVariableQuery() string {
	return (query{
		form:   labelsForm,
		metric: metricRequests,
		label:  "pod_id",
		matchers: []labelMatcher{
			{label: "job", op: "=", value: prometheusJob},
			{label: "datacenter", op: "=~", value: datacenterVar, raw: true},
		},
	}).String()
}

// promqlString quotes value as a PromQL double-quoted literal.
// JSON encoding of the surrounding document is a separate step.
func promqlString(value string) string {
	var b strings.Builder
	b.Grow(len(value) + 2)
	b.WriteByte('"')
	for _, r := range value {
		switch r {
		case '\\', '"':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 || r == 0x7f {
				writeUnicodeEscape(&b, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

func writeUnicodeEscape(b *strings.Builder, r rune) {
	const hexdigits = "0123456789abcdef"
	b.WriteString(`\u`)
	b.WriteByte(hexdigits[(r>>12)&0xF])
	b.WriteByte(hexdigits[(r>>8)&0xF])
	b.WriteByte(hexdigits[(r>>4)&0xF])
	b.WriteByte(hexdigits[r&0xF])
}
