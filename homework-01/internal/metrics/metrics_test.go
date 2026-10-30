package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func newTestRouter(t *testing.T) (*Router, *http.ServeMux, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	mux := http.NewServeMux()
	r := NewRouter(reg, mux)
	r.Handle("GET /items/{id}", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.PathValue("id") == "404" {
			http.NotFound(w, req)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	r.Handle("POST /items", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	return r, mux, reg
}

func serve(mux http.Handler, method, path string) {
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, path, nil))
}

func TestRequestsCountedByRouteTemplateAndCode(t *testing.T) {
	r, mux, _ := newTestRouter(t)

	serve(mux, http.MethodGet, "/items/1")
	serve(mux, http.MethodGet, "/items/2")
	serve(mux, http.MethodGet, "/items/404")
	serve(mux, http.MethodPost, "/items")

	cases := []struct {
		labels []string
		want   float64
	}{
		{[]string{"GET", "/items/{id}", "200"}, 2},
		{[]string{"GET", "/items/{id}", "404"}, 1},
		{[]string{"POST", "/items", "201"}, 1},
	}
	for _, c := range cases {
		if got := testutil.ToFloat64(r.requests.WithLabelValues(c.labels...)); got != c.want {
			t.Errorf("requests%v = %v, want %v", c.labels, got, c.want)
		}
	}
}

func TestDurationHistogram(t *testing.T) {
	_, mux, reg := newTestRouter(t)

	serve(mux, http.MethodGet, "/items/1")
	serve(mux, http.MethodGet, "/items/404")

	n, err := testutil.GatherAndCount(reg, RequestDurationName)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected 2 histogram series, got %d", n)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() != RequestDurationName {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			wantCount := uint64(0)
			if labels[LabelRoute] == "/items/{id}" {
				wantCount = 2
			}
			if got := m.GetHistogram().GetSampleCount(); got != wantCount {
				t.Errorf("%v: sample count %d, want %d", labels, got, wantCount)
			}
		}
	}
}

func TestConstLabelsFromWrappedRegisterer(t *testing.T) {
	reg := prometheus.NewRegistry()
	wrapped := prometheus.WrapRegistererWith(prometheus.Labels{"pod": "pod-1", "datacenter": "dc-a"}, reg)
	mux := http.NewServeMux()
	NewRouter(wrapped, mux).Handle("GET /x", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	serve(mux, http.MethodGet, "/x")

	const want = `
# HELP http_requests_total Total number of HTTP requests by handler and response code.
# TYPE http_requests_total counter
http_requests_total{code="200",datacenter="dc-a",method="GET",pod="pod-1",route="/x"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), RequestsTotalName); err != nil {
		t.Fatal(err)
	}
}

func TestHandlersEndpoint(t *testing.T) {
	r, _, _ := newTestRouter(t)

	rec := httptest.NewRecorder()
	r.HandlersHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/handlers", nil))

	var body struct {
		Handlers []Handler `json:"handlers"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	want := []Handler{{"GET", "/items/{id}"}, {"POST", "/items"}}
	if !reflect.DeepEqual(body.Handlers, want) {
		t.Fatalf("handlers = %+v, want %+v", body.Handlers, want)
	}
}

func TestHandlePanicsOnPatternWithoutMethod(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	NewRouter(prometheus.NewRegistry(), http.NewServeMux()).Handle("/items", http.NotFoundHandler())
}
