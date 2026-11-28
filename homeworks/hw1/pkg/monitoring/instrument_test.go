package monitoring

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestValidateIdentity(t *testing.T) {
	t.Parallel()

	valid := []string{"a", "1", "pod-a", "dc-1", "us-east-1", "pod.a", "Pod_1", strings.Repeat("b", 63)}
	for _, value := range valid {
		if err := ValidateIdentity(value, value); err != nil {
			t.Fatalf("ValidateIdentity(%q) unexpected error: %v", value, err)
		}
	}

	invalid := []struct {
		name       string
		pod        string
		datacenter string
		want       string
	}{
		{name: "empty pod", datacenter: "dc-a", want: "POD_ID is required"},
		{name: "empty datacenter", pod: "pod-a", want: "DATACENTER is required"},
		{name: "space", pod: "pod a", datacenter: "dc-a", want: "POD_ID"},
		{name: "slash", pod: "pod/a", datacenter: "dc-a", want: "POD_ID"},
		{name: "newline", pod: "pod-a", datacenter: "dc\na", want: "DATACENTER"},
		{name: "trailing hyphen", pod: "pod-", datacenter: "dc-a", want: "POD_ID"},
		{name: "too long", pod: strings.Repeat("a", 64), datacenter: "dc-a", want: "POD_ID"},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateIdentity(test.pod, test.datacenter)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want text %q", err, test.want)
			}
		})
	}
}

func TestDurationBucketsAreFixed(t *testing.T) {
	t.Parallel()

	buckets := DurationBuckets()
	if len(buckets) < 2 {
		t.Fatal("expected several finite buckets")
	}
	for i, bound := range buckets {
		if bound <= 0 || math.IsInf(bound, 0) {
			t.Fatalf("bucket %d = %v", i, bound)
		}
		if i > 0 && bound <= buckets[i-1] {
			t.Fatalf("bucket %d is not increasing", i)
		}
	}
	buckets[0] = -1
	if DurationBuckets()[0] == -1 {
		t.Fatal("DurationBuckets shares its backing array")
	}
}

func TestNewMetricsDoesNotObserve(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	if _, err := NewMetrics(reg, "pod-a", "dc-a"); err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	if samples := sampleCount(t, reg); samples != 0 {
		t.Fatalf("samples at startup = %d", samples)
	}
}

func TestNewMetricsRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	if _, err := NewMetrics(nil, "pod-a", "dc-a"); err == nil {
		t.Fatal("nil registry was accepted")
	}
	if _, err := NewMetrics(reg, "", "dc-a"); err == nil {
		t.Fatal("empty pod id was accepted")
	}
	if _, err := NewMetrics(reg, "pod-a", "dc-a"); err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	if _, err := NewMetrics(reg, "pod-a", "dc-a"); err == nil {
		t.Fatal("duplicate metrics registration was accepted")
	}
}

func TestMetricsIgnoreEnvironment(t *testing.T) {
	t.Setenv("POD_ID", "from-env")
	t.Setenv("DATACENTER", "from-env")
	t.Setenv("LATENCY_BUCKETS", "1,2,3")

	reg := NewRegistry()
	metrics := newTestMetrics(t, reg, "pod-arg", "dc-arg")
	metrics.wrap("GET", "/items/{id}", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), panicStatus(http.StatusInternalServerError)).ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/items/from-env", nil),
	)

	labels := labelsOf(t, reg, RequestsTotal)
	if len(labels) != 1 {
		t.Fatalf("series = %d", len(labels))
	}
	if labels[0]["pod_id"] != "pod-arg" || labels[0]["datacenter"] != "dc-arg" {
		t.Fatalf("labels = %v", labels[0])
	}
	if !slices.Equal(histogramBounds(t, reg), DurationBuckets()) {
		t.Fatalf("bounds = %v", histogramBounds(t, reg))
	}
}

func TestHistogramBucketsMatchAcrossRegistries(t *testing.T) {
	t.Parallel()

	firstReg := NewRegistry()
	secondReg := NewRegistry()
	first := newTestMetrics(t, firstReg, "pod-a", "dc-a")
	second := newTestMetrics(t, secondReg, "pod-b", "dc-b")
	observeOne(first)
	observeOne(second)

	if !slices.Equal(histogramBounds(t, firstReg), DurationBuckets()) {
		t.Fatalf("first bounds = %v", histogramBounds(t, firstReg))
	}
	if !slices.Equal(histogramBounds(t, firstReg), histogramBounds(t, secondReg)) {
		t.Fatal("registries expose different finite buckets")
	}

	firstLE := leValues(scrape(t, first))
	secondLE := leValues(scrape(t, second))
	if !slices.Equal(firstLE, secondLE) {
		t.Fatalf("le sets differ: %v vs %v", firstLE, secondLE)
	}
	if len(firstLE) == 0 || firstLE[len(firstLE)-1] != "+Inf" {
		t.Fatalf("le values = %v", firstLE)
	}
	if len(firstLE) != len(DurationBuckets())+1 {
		t.Fatalf("le count = %d, buckets %d", len(firstLE), len(DurationBuckets()))
	}
}

func TestRegistrationFlagsAreIndependent(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	metrics := newTestMetrics(t, reg, "pod-a", "dc-a")
	routes := newTestRegistrar(t, metrics)
	mux := http.NewServeMux()
	business := Options{Instrument: true, Catalog: true}
	if err := routes.Handle(mux, http.MethodPost, "/items", statusHandler(http.StatusCreated), business); err != nil {
		t.Fatalf("register business route: %v", err)
	}
	if err := routes.Handle(mux, http.MethodGet, "/handlers", routes, Options{Instrument: true, Catalog: false}); err != nil {
		t.Fatalf("register handlers: %v", err)
	}
	if err := routes.Handle(mux, http.MethodGet, "/plain", statusHandler(http.StatusNoContent), Options{}); err != nil {
		t.Fatalf("register plain route: %v", err)
	}

	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/plain", nil))
	if samples := sampleCount(t, reg); samples != 0 {
		t.Fatalf("non-instrumented route created %d samples", samples)
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/handlers", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("handlers status = %d", rec.Code)
	}
	var body struct {
		Handlers []Endpoint `json:"handlers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode handlers: %v", err)
	}
	if len(body.Handlers) != 1 || body.Handlers[0] != (Endpoint{Method: http.MethodPost, Path: "/items"}) {
		t.Fatalf("catalog = %+v", body.Handlers)
	}
	requireCounter(t, reg, map[string]string{
		"pod_id": "pod-a", "datacenter": "dc-a", "method": http.MethodGet, "route": "/handlers", "status_code": "200",
	}, 1)
	requireHistogramCount(t, reg, map[string]string{
		"pod_id": "pod-a", "datacenter": "dc-a", "method": http.MethodGet, "route": "/handlers",
	}, 1)

	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/items", nil))
	requireCounter(t, reg, map[string]string{
		"pod_id": "pod-a", "datacenter": "dc-a", "method": http.MethodPost, "route": "/items", "status_code": "201",
	}, 1)
	if got := routes.Catalog(); len(got) != 1 || got[0].Path != "/items" {
		t.Fatalf("catalog = %+v", got)
	}
}

func TestRouteTemplateIsClosedOver(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	metrics := newTestMetrics(t, reg, "pod-a", "dc-a")
	handler := metrics.wrap("PUT", "/items/{id}", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}), panicStatus(http.StatusInternalServerError))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/items/id-should-not-leak?token=should-not-leak", nil))

	requireCounter(t, reg, map[string]string{
		"pod_id": "pod-a", "datacenter": "dc-a", "method": http.MethodPut, "route": "/items/{id}", "status_code": "200",
	}, 1)
	if strings.Contains(scrape(t, metrics), "should-not-leak") {
		t.Fatal("request id or query leaked into metrics")
	}
	assertBoundedLabels(t, reg)
}

func TestStatusAccounting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		handler http.HandlerFunc
		want    string
		codes   []int
	}{
		{
			name: "implicit 200 without a write",
			handler: func(http.ResponseWriter, *http.Request) {
			},
			want: "200",
		},
		{
			name: "implicit 200 on write",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, "body")
			},
			want: "200",
		},
		{
			name: "explicit 201",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusCreated)
			},
			want:  "201",
			codes: []int{http.StatusCreated},
		},
		{
			name: "first final status wins",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusCreated)
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = io.WriteString(w, "created")
			},
			want:  "201",
			codes: []int{http.StatusCreated},
		},
		{
			name: "informational status is not final",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusContinue)
				w.WriteHeader(http.StatusNoContent)
			},
			want:  "204",
			codes: []int{http.StatusContinue, http.StatusNoContent},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			reg := NewRegistry()
			metrics := newTestMetrics(t, reg, "pod-a", "dc-a")
			writer := &recordingWriter{}
			metrics.wrap("GET", "/probe", test.handler, panicStatus(http.StatusInternalServerError)).ServeHTTP(
				writer,
				httptest.NewRequest(http.MethodGet, "/probe", nil),
			)
			requireCounter(t, reg, map[string]string{
				"pod_id": "pod-a", "datacenter": "dc-a", "method": "GET", "route": "/probe", "status_code": test.want,
			}, 1)
			requireHistogramCount(t, reg, map[string]string{
				"pod_id": "pod-a", "datacenter": "dc-a", "method": "GET", "route": "/probe",
			}, 1)
			if series := len(labelsOf(t, reg, RequestsTotal)); series != 1 {
				t.Fatalf("counter series = %d", series)
			}
			if test.codes != nil && !slices.Equal(writer.codes, test.codes) {
				t.Fatalf("forwarded codes = %v, want %v", writer.codes, test.codes)
			}
		})
	}
}

func TestErrorCodesAreObserved(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	metrics := newTestMetrics(t, reg, "pod-a", "dc-a")
	handler := metrics.wrap("GET", "/items/{id}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status, err := strconv.Atoi(r.URL.Query().Get("status"))
		if err != nil {
			t.Errorf("status query: %v", err)
			return
		}
		http.Error(w, "secret-error-text", status)
	}), panicStatus(http.StatusInternalServerError))

	for _, status := range []int{http.StatusBadRequest, http.StatusNotFound, http.StatusInternalServerError} {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/items/id-should-not-leak?status="+strconv.Itoa(status), nil))
		requireCounter(t, reg, map[string]string{
			"pod_id": "pod-a", "datacenter": "dc-a", "method": "GET", "route": "/items/{id}", "status_code": strconv.Itoa(status),
		}, 1)
	}
	requireHistogramCount(t, reg, map[string]string{
		"pod_id": "pod-a", "datacenter": "dc-a", "method": "GET", "route": "/items/{id}",
	}, 3)
	body := scrape(t, metrics)
	if strings.Contains(body, "secret-error-text") || strings.Contains(body, "id-should-not-leak") || strings.Contains(body, "status=") {
		t.Fatal("unbounded value leaked into metrics")
	}
	if strings.Contains(body, "http_errors_total") {
		t.Fatal("separate error counter was exported")
	}
}

func TestDurationMatchesHandlerTime(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	metrics := newTestMetrics(t, reg, "pod-a", "dc-a")
	metrics.wrap("GET", "/slow", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}), panicStatus(http.StatusInternalServerError)).ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/slow", nil),
	)

	histogram := histogramOf(t, reg, map[string]string{
		"pod_id": "pod-a", "datacenter": "dc-a", "method": "GET", "route": "/slow",
	})
	if histogram.GetSampleCount() != 1 {
		t.Fatalf("count = %d", histogram.GetSampleCount())
	}
	if histogram.GetSampleSum() < 0.015 {
		t.Fatalf("sum = %v", histogram.GetSampleSum())
	}
	for _, bucket := range histogram.GetBucket() {
		if bucket.GetUpperBound() == 0.005 && bucket.GetCumulativeCount() != 0 {
			t.Fatal("slow handler was recorded below 5ms")
		}
	}
}

func TestPanicBeforeWriteRecords500(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	metrics := newTestMetrics(t, reg, "pod-a", "dc-a")
	handler := metrics.wrap("GET", "/boom", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("secret-panic-token")
	}), panicStatus(http.StatusInternalServerError))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/boom", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", rec.Code)
	}
	requireCounter(t, reg, map[string]string{
		"pod_id": "pod-a", "datacenter": "dc-a", "method": "GET", "route": "/boom", "status_code": "500",
	}, 1)
	requireHistogramCount(t, reg, map[string]string{
		"pod_id": "pod-a", "datacenter": "dc-a", "method": "GET", "route": "/boom",
	}, 1)
	if strings.Contains(scrape(t, metrics), "secret-panic-token") {
		t.Fatal("panic value leaked into metrics")
	}
}

func TestPanicAfterHeadersDoesNotReplaceMetricStatus(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	metrics := newTestMetrics(t, reg, "pod-a", "dc-a")
	writer := &recordingWriter{}
	metrics.wrap("GET", "/partial", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "partial")
		panic("secret-panic-token")
	}), func(w http.ResponseWriter, _ *http.Request, _ any, _ bool) {
		w.WriteHeader(http.StatusInternalServerError)
	}).ServeHTTP(writer, httptest.NewRequest(http.MethodGet, "/partial", nil))

	if !slices.Equal(writer.codes, []int{http.StatusCreated}) || writer.body.String() != "partial" {
		t.Fatalf("codes = %v, body = %q", writer.codes, writer.body.String())
	}
	requireCounter(t, reg, map[string]string{
		"pod_id": "pod-a", "datacenter": "dc-a", "method": "GET", "route": "/partial", "status_code": "201",
	}, 1)
	if _, ok := counterValue(reg, map[string]string{"status_code": "500"}); ok {
		t.Fatal("panic after commit was counted as 500")
	}
}

func TestResponseControllerFlush(t *testing.T) {
	t.Parallel()

	metrics := newTestMetrics(t, NewRegistry(), "pod-a", "dc-a")
	handler := metrics.wrap("GET", "/flush", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush: %v", err)
		}
	}), panicStatus(http.StatusInternalServerError))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/flush", nil))
	if !rec.Flushed {
		t.Fatal("response was not flushed through Unwrap")
	}
}

func TestConcurrentObservationsShareOneSeries(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	metrics := newTestMetrics(t, reg, "pod-a", "dc-a")
	handler := metrics.wrap("GET", "/items/{id}", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), panicStatus(http.StatusInternalServerError))

	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			target := "/items/id-" + strconv.Itoa(i) + "?q=secret"
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, target, nil))
		}(i)
	}
	wg.Wait()

	requireCounter(t, reg, map[string]string{
		"pod_id": "pod-a", "datacenter": "dc-a", "method": "GET", "route": "/items/{id}", "status_code": "200",
	}, 40)
	if series := len(labelsOf(t, reg, RequestsTotal)); series != 1 {
		t.Fatalf("counter series = %d", series)
	}
}

func TestRegistrarRejectsBadRoutes(t *testing.T) {
	t.Parallel()

	metrics := newTestMetrics(t, NewRegistry(), "pod-a", "dc-a")
	routes := newTestRegistrar(t, metrics)
	mux := http.NewServeMux()
	opts := Options{Instrument: true, Catalog: true}
	valid := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	if err := routes.Handle(mux, http.MethodGet, "/items/{id}", valid, opts); err != nil {
		t.Fatalf("valid route: %v", err)
	}
	rejected := []struct {
		method string
		path   string
	}{
		{method: "get", path: "/items"},
		{method: "", path: "/items"},
		{method: http.MethodGet, path: "items"},
		{method: http.MethodGet, path: "/items?x=1"},
		{method: http.MethodGet, path: "http://example.test/items"},
		{method: http.MethodGet, path: "/items/{id}"},
	}
	for _, route := range rejected {
		if err := routes.Handle(mux, route.method, route.path, valid, opts); err == nil {
			t.Fatalf("accepted %s %s", route.method, route.path)
		}
	}
	if _, err := NewRegistrar(nil, panicStatus(http.StatusInternalServerError)); err == nil {
		t.Fatal("nil metrics was accepted")
	}
	if _, err := NewRegistrar(metrics, nil); err == nil {
		t.Fatal("nil panic handler was accepted")
	}
}

func newTestMetrics(t *testing.T, reg *prometheus.Registry, podID, datacenter string) *Metrics {
	t.Helper()
	metrics, err := NewMetrics(reg, podID, datacenter)
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	return metrics
}

func newTestRegistrar(t *testing.T, metrics *Metrics) *Registrar {
	t.Helper()
	routes, err := NewRegistrar(metrics, panicStatus(http.StatusInternalServerError))
	if err != nil {
		t.Fatalf("new registrar: %v", err)
	}
	return routes
}

func panicStatus(status int) PanicHandler {
	return func(w http.ResponseWriter, _ *http.Request, _ any, wroteHeader bool) {
		if wroteHeader {
			return
		}
		w.WriteHeader(status)
	}
}

func statusHandler(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}
}

func observeOne(metrics *Metrics) {
	metrics.wrap("GET", "/items/{id}", statusHandler(http.StatusOK), panicStatus(http.StatusInternalServerError)).ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/items/1", nil),
	)
}

func scrape(t *testing.T, metrics *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metrics.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", rec.Code)
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("content type = %q", rec.Header().Get("Content-Type"))
	}
	return rec.Body.String()
}

var leValuePattern = regexp.MustCompile(`\ble="([^"]*)"`)

func leValues(body string) []string {
	seen := map[string]struct{}{}
	var values []string
	for _, match := range leValuePattern.FindAllStringSubmatch(body, -1) {
		if _, ok := seen[match[1]]; ok {
			continue
		}
		seen[match[1]] = struct{}{}
		values = append(values, match[1])
	}
	return values
}

func sampleCount(t *testing.T, reg *prometheus.Registry) int {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	total := 0
	for _, family := range families {
		total += len(family.GetMetric())
	}
	return total
}

func requireCounter(t *testing.T, reg *prometheus.Registry, labels map[string]string, want float64) {
	t.Helper()
	got, ok := counterValue(reg, labels)
	if !ok || got != want {
		t.Fatalf("counter %v = %v (found %v), want %v", labels, got, ok, want)
	}
}

func counterValue(reg *prometheus.Registry, labels map[string]string) (float64, bool) {
	for _, family := range mustGather(reg) {
		if family.GetName() != RequestsTotal {
			continue
		}
		for _, metric := range family.GetMetric() {
			if matchLabels(metric, labels) && metric.GetCounter() != nil {
				return metric.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}

func requireHistogramCount(t *testing.T, reg *prometheus.Registry, labels map[string]string, want uint64) {
	t.Helper()
	histogram := histogramOf(t, reg, labels)
	if histogram.GetSampleCount() != want {
		t.Fatalf("histogram %v count = %d, want %d", labels, histogram.GetSampleCount(), want)
	}
}

func histogramOf(t *testing.T, reg *prometheus.Registry, labels map[string]string) *dto.Histogram {
	t.Helper()
	for _, family := range mustGather(reg) {
		if family.GetName() != RequestDurationSeconds {
			continue
		}
		for _, metric := range family.GetMetric() {
			if matchLabels(metric, labels) && metric.GetHistogram() != nil {
				return metric.GetHistogram()
			}
		}
	}
	t.Fatalf("histogram %v not found", labels)
	return nil
}

func histogramBounds(t *testing.T, reg *prometheus.Registry) []float64 {
	t.Helper()
	for _, family := range mustGather(reg) {
		if family.GetName() != RequestDurationSeconds || len(family.GetMetric()) == 0 {
			continue
		}
		buckets := family.GetMetric()[0].GetHistogram().GetBucket()
		bounds := make([]float64, len(buckets))
		for i, bucket := range buckets {
			bounds[i] = bucket.GetUpperBound()
		}
		return bounds
	}
	t.Fatal("histogram bounds not found")
	return nil
}

func labelsOf(t *testing.T, reg *prometheus.Registry, name string) []map[string]string {
	t.Helper()
	var out []map[string]string
	for _, family := range mustGather(reg) {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			out = append(out, labels)
		}
	}
	return out
}

func assertBoundedLabels(t *testing.T, reg *prometheus.Registry) {
	t.Helper()
	allowed := map[string]bool{
		"pod_id": true, "datacenter": true, "method": true, "route": true, "status_code": true,
	}
	for _, family := range mustGather(reg) {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if !allowed[label.GetName()] {
					t.Fatalf("unexpected label %s", label.GetName())
				}
			}
		}
	}
}

func matchLabels(metric *dto.Metric, want map[string]string) bool {
	got := map[string]string{}
	for _, label := range metric.GetLabel() {
		got[label.GetName()] = label.GetValue()
	}
	for key, value := range want {
		if got[key] != value {
			return false
		}
	}
	return true
}

func mustGather(reg *prometheus.Registry) []*dto.MetricFamily {
	families, err := reg.Gather()
	if err != nil {
		panic(err)
	}
	return families
}

type recordingWriter struct {
	header http.Header
	codes  []int
	body   strings.Builder
}

func (w *recordingWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *recordingWriter) WriteHeader(code int) {
	w.codes = append(w.codes, code)
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	return w.body.WriteString(string(p))
}
