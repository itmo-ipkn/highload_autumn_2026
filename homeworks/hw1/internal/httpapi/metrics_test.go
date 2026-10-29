package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"highload/lab1/internal/item"
	"highload/lab1/pkg/monitoring"
)

func TestStartupDoesNotCreateMetricSamples(t *testing.T) {
	t.Parallel()

	reg := monitoring.NewRegistry()
	api := newReadyAPI(t, reg, item.NewStore())
	if samples := sampleCount(t, reg); samples != 0 {
		t.Fatalf("samples after construction = %d", samples)
	}

	handler := api.Handler()
	calls := []struct {
		method string
		path   string
		want   int
	}{
		{method: http.MethodGet, path: "/health", want: http.StatusOK},
		{method: http.MethodGet, path: "/metrics", want: http.StatusOK},
		{method: http.MethodGet, path: "/items", want: http.StatusMethodNotAllowed},
		{method: http.MethodDelete, path: "/items/id-should-not-leak", want: http.StatusMethodNotAllowed},
	}
	for _, call := range calls {
		rec := performRaw(handler, call.method, call.path, "")
		if rec.Code != call.want {
			t.Fatalf("%s %s status = %d", call.method, call.path, rec.Code)
		}
	}
	if samples := sampleCount(t, reg); samples != 0 {
		t.Fatalf("excluded routes created %d samples", samples)
	}
	body := scrape(t, handler)
	if strings.Contains(body, monitoring.RequestsTotal) || strings.Contains(body, "id-should-not-leak") {
		t.Fatalf("metrics body = %s", body)
	}
}

func TestHandlersCatalogExcludesServiceRoutes(t *testing.T) {
	t.Parallel()

	reg := monitoring.NewRegistry()
	handler := newReadyAPI(t, reg, item.NewStore()).Handler()
	rec := performRaw(handler, http.MethodGet, "/handlers", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("content type = %q", rec.Header().Get("Content-Type"))
	}
	var body struct {
		Handlers []struct {
			Method string `json:"method"`
			Path   string `json:"path"`
		} `json:"handlers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := []string{"POST /items", "GET /items/{id}", "PUT /items/{id}"}
	got := make([]string, len(body.Handlers))
	for i, endpoint := range body.Handlers {
		got[i] = endpoint.Method + " " + endpoint.Path
	}
	if !slices.Equal(got, want) {
		t.Fatalf("catalog = %v", got)
	}

	requireCounter(t, reg, map[string]string{
		"pod_id": "pod-test", "datacenter": "dc-test", "method": "GET", "route": "/handlers", "status_code": "200",
	}, 1)
	requireHistogramCount(t, reg, map[string]string{
		"pod_id": "pod-test", "datacenter": "dc-test", "method": "GET", "route": "/handlers",
	}, 1)
	for _, route := range []string{"/items", "/items/{id}", "/health", "/metrics"} {
		if _, ok := counterValue(reg, map[string]string{"route": route}); ok {
			t.Fatalf("route %s was counted before a business request", route)
		}
	}
}

func TestBusinessRoutesUseTemplatesAndErrorCodes(t *testing.T) {
	t.Parallel()

	reg := monitoring.NewRegistry()
	handler := newReadyAPI(t, reg, mixedStore{inner: item.NewStore()}).Handler()

	invalidPost := performRaw(handler, http.MethodPost, "/items", "{")
	if invalidPost.Code != http.StatusBadRequest {
		t.Fatalf("invalid post status = %d", invalidPost.Code)
	}
	created := performRaw(handler, http.MethodPost, "/items", `{"name":"example","value":"some value"}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body %s", created.Code, created.Body.String())
	}
	var itemBody struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &itemBody); err != nil {
		t.Fatalf("decode item: %v", err)
	}

	got := performRaw(handler, http.MethodGet, "/items/"+itemBody.ID+"?token=should-not-leak", "")
	if got.Code != http.StatusOK {
		t.Fatalf("get status = %d", got.Code)
	}
	missing := performRaw(handler, http.MethodGet, "/items/id-should-not-leak", "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d", missing.Code)
	}
	invalid := performRaw(handler, http.MethodPut, "/items/id-should-not-leak", "{")
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid put status = %d", invalid.Code)
	}
	failed := performRaw(handler, http.MethodGet, "/items/force-error-id", "")
	if failed.Code != http.StatusInternalServerError || strings.Contains(failed.Body.String(), "secret-disk-failure") {
		t.Fatalf("failed status = %d, body %s", failed.Code, failed.Body.String())
	}

	requireCounter(t, reg, map[string]string{"method": "POST", "route": "/items", "status_code": "201"}, 1)
	requireCounter(t, reg, map[string]string{"method": "POST", "route": "/items", "status_code": "400"}, 1)
	requireCounter(t, reg, map[string]string{"method": "GET", "route": "/items/{id}", "status_code": "200"}, 1)
	requireCounter(t, reg, map[string]string{"method": "GET", "route": "/items/{id}", "status_code": "404"}, 1)
	requireCounter(t, reg, map[string]string{"method": "PUT", "route": "/items/{id}", "status_code": "400"}, 1)
	requireCounter(t, reg, map[string]string{"method": "GET", "route": "/items/{id}", "status_code": "500"}, 1)
	requireHistogramCount(t, reg, map[string]string{"method": "GET", "route": "/items/{id}"}, 3)
	requireHistogramCount(t, reg, map[string]string{"method": "POST", "route": "/items"}, 2)
	requireHistogramCount(t, reg, map[string]string{"method": "PUT", "route": "/items/{id}"}, 1)

	body := scrape(t, handler)
	for _, forbidden := range []string{itemBody.ID, "should-not-leak", "force-error-id", "secret-disk-failure", `route="/health"`, `route="/metrics"`} {
		if forbidden != "" && strings.Contains(body, forbidden) {
			t.Fatalf("metrics contain %q", forbidden)
		}
	}
	if !strings.Contains(body, `le="+Inf"`) {
		t.Fatal("histogram exposition has no +Inf bucket")
	}
	assertFamilyNames(t, reg)
}

func TestPanicOnBusinessRouteRecords500(t *testing.T) {
	t.Parallel()

	reg := monitoring.NewRegistry()
	handler := newReadyAPI(t, reg, panicStore{}).Handler()
	rec := performRaw(handler, http.MethodGet, "/items/id-should-not-leak", "")
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "secret-panic-token") {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "internal error") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	requireCounter(t, reg, map[string]string{"method": "GET", "route": "/items/{id}", "status_code": "500"}, 1)
	requireHistogramCount(t, reg, map[string]string{"method": "GET", "route": "/items/{id}"}, 1)
	if strings.Contains(scrape(t, handler), "secret-panic-token") {
		t.Fatal("panic value leaked into metrics")
	}
}

func TestPanicAfterHeadersKeepsMetricsStatus(t *testing.T) {
	t.Parallel()

	reg := monitoring.NewRegistry()
	metrics := testMetricsOn(t, reg, "pod-test", "dc-test")
	routes := newRoutes(t, metrics)
	mux := http.NewServeMux()
	err := routes.Handle(mux, http.MethodGet, "/partial", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "partial")
		panic("secret-panic-token")
	}), monitoring.Options{Instrument: true, Catalog: false})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	rec := httptest.NewRecorder()
	withRecovery(mux).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/partial", nil))
	if rec.Code != http.StatusCreated || rec.Body.String() != "partial" || strings.Contains(rec.Body.String(), "secret-panic-token") {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	requireCounter(t, reg, map[string]string{"method": "GET", "route": "/partial", "status_code": "201"}, 1)
	requireHistogramCount(t, reg, map[string]string{"method": "GET", "route": "/partial"}, 1)
	if _, ok := counterValue(reg, map[string]string{"status_code": "500"}); ok {
		t.Fatal("committed 201 was replaced with 500 in metrics")
	}
}

func TestResponseControllerFlushThroughAPI(t *testing.T) {
	t.Parallel()

	metrics := testMetrics(t)
	routes := newRoutes(t, metrics)
	mux := http.NewServeMux()
	err := routes.Handle(mux, http.MethodGet, "/flush", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if flushErr := http.NewResponseController(w).Flush(); flushErr != nil {
			t.Errorf("flush: %v", flushErr)
		}
	}), monitoring.Options{Instrument: true})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	rec := httptest.NewRecorder()
	withRecovery(mux).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/flush", nil))
	if !rec.Flushed {
		t.Fatal("flush did not reach the underlying writer")
	}
}

func TestInformationalStatusStaysRewritable(t *testing.T) {
	t.Parallel()

	reg := monitoring.NewRegistry()
	metrics := testMetricsOn(t, reg, "pod-test", "dc-test")
	routes := newRoutes(t, metrics)
	mux := http.NewServeMux()
	err := routes.Handle(mux, http.MethodGet, "/continue", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusContinue)
		w.WriteHeader(http.StatusNoContent)
	}), monitoring.Options{Instrument: true})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	writer := &recordingWriter{}
	withRecovery(mux).ServeHTTP(writer, httptest.NewRequest(http.MethodGet, "/continue", nil))
	if !slices.Equal(writer.codes, []int{http.StatusContinue, http.StatusNoContent}) {
		t.Fatalf("codes = %v", writer.codes)
	}
	requireCounter(t, reg, map[string]string{"method": "GET", "route": "/continue", "status_code": "204"}, 1)
}

var errSecret = errString("secret-disk-failure")

type errString string

func (e errString) Error() string { return string(e) }

type mixedStore struct {
	inner *item.Store
}

func (m mixedStore) Create(name, value string) (item.Item, error) {
	return m.inner.Create(name, value)
}

func (m mixedStore) Get(id string) (item.Item, error) {
	if id == "force-error-id" {
		return item.Item{}, errSecret
	}
	return m.inner.Get(id)
}

func (m mixedStore) Update(id, name, value string) (item.Item, error) {
	return m.inner.Update(id, name, value)
}

type panicStore struct{}

func (panicStore) Create(string, string) (item.Item, error) {
	panic("secret-panic-token")
}

func (panicStore) Get(string) (item.Item, error) {
	panic("secret-panic-token")
}

func (panicStore) Update(string, string, string) (item.Item, error) {
	panic("secret-panic-token")
}

func testMetrics(t *testing.T) *monitoring.Metrics {
	t.Helper()
	return testMetricsOn(t, monitoring.NewRegistry(), "pod-test", "dc-test")
}

func testMetricsOn(t *testing.T, reg *prometheus.Registry, podID, datacenter string) *monitoring.Metrics {
	t.Helper()
	metrics, err := monitoring.NewMetrics(reg, podID, datacenter)
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	return metrics
}

func newRoutes(t *testing.T, metrics *monitoring.Metrics) *monitoring.Registrar {
	t.Helper()
	routes, err := monitoring.NewRegistrar(metrics, recoverHTTP)
	if err != nil {
		t.Fatalf("registrar: %v", err)
	}
	return routes
}

func newReadyAPI(t *testing.T, reg *prometheus.Registry, store repository) *API {
	t.Helper()
	health := NewHealth()
	health.SetReady(true)
	api, err := New(store, health, 1024, testMetricsOn(t, reg, "pod-test", "dc-test"))
	if err != nil {
		t.Fatalf("new api: %v", err)
	}
	return api
}

func scrape(t *testing.T, handler http.Handler) string {
	t.Helper()
	rec := performRaw(handler, http.MethodGet, "/metrics", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("content type = %q", rec.Header().Get("Content-Type"))
	}
	return rec.Body.String()
}

func sampleCount(t *testing.T, reg *prometheus.Registry) int {
	t.Helper()
	total := 0
	for _, family := range gather(t, reg) {
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
	families, err := reg.Gather()
	if err != nil {
		return 0, false
	}
	for _, family := range families {
		if family.GetName() != monitoring.RequestsTotal {
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
	for _, family := range gather(t, reg) {
		if family.GetName() != monitoring.RequestDurationSeconds {
			continue
		}
		for _, metric := range family.GetMetric() {
			if matchLabels(metric, labels) && metric.GetHistogram() != nil {
				if metric.GetHistogram().GetSampleCount() != want {
					t.Fatalf("histogram %v count = %d, want %d", labels, metric.GetHistogram().GetSampleCount(), want)
				}
				return
			}
		}
	}
	t.Fatalf("histogram %v not found", labels)
}

func assertFamilyNames(t *testing.T, reg *prometheus.Registry) {
	t.Helper()
	names := map[string]struct{}{}
	for _, family := range gather(t, reg) {
		names[family.GetName()] = struct{}{}
	}
	if len(names) != 2 {
		t.Fatalf("families = %v", names)
	}
	if _, ok := names[monitoring.RequestsTotal]; !ok {
		t.Fatal("request counter is missing")
	}
	if _, ok := names[monitoring.RequestDurationSeconds]; !ok {
		t.Fatal("duration histogram is missing")
	}
}

func gather(t *testing.T, reg *prometheus.Registry) []*dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	return families
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

type recordingWriter struct {
	header http.Header
	codes  []int
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
	if len(w.codes) == 0 {
		w.codes = append(w.codes, http.StatusOK)
	}
	return len(p), nil
}
