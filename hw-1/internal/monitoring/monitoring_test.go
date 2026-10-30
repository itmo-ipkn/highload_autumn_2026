package monitoring

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func TestStatusAndRecovery(t *testing.T) {
	m := New("pod", "dc")
	router := chi.NewRouter()
	measured := router.With(m.Middleware, middleware.Recoverer)
	measured.Get("/empty", func(w http.ResponseWriter, r *http.Request) {})
	measured.Get("/fail", func(w http.ResponseWriter, r *http.Request) { panic("test failure") })
	measured.Get("/explicit", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) })
	measured.Get("/committed", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(202)
		panic("failure after headers") // Отправленный статус уже нельзя заменить на 500.
	})
	cases := map[string]int{"/empty": 200, "/fail": 500, "/explicit": 503, "/committed": 202}
	for path, want := range cases {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		if response.Code != want {
			t.Errorf("%s: got %d, want %d", path, response.Code, want)
		}
	}
	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "items_api_requests_total" {
			continue
		}
		if len(family.Metric) != len(cases) {
			t.Fatalf("counted %d series", len(family.Metric))
		}
		for _, metric := range family.Metric {
			var path, status string
			for _, l := range metric.Label {
				if l.GetName() == "route" {
					path = l.GetValue()
				}
				if l.GetName() == "status_code" {
					status = l.GetValue()
				}
			}
			expected := map[string]string{"/empty": "200", "/fail": "500", "/explicit": "503", "/committed": "202"}
			if status != expected[path] || metric.GetCounter().GetValue() != 1 {
				t.Errorf("incorrect observation for %s: %s", path, status)
			}
		}
	}
}

func TestCatalogDiscoversNewNestedRoute(t *testing.T) {
	router := chi.NewRouter()
	router.Route("/v1", func(r chi.Router) {
		r.Patch("/widgets/{key}", func(w http.ResponseWriter, r *http.Request) {})
	})
	handlers := Catalog(router)
	if len(handlers) != 1 || handlers[0] != (Handler{Method: "PATCH", Path: "/v1/widgets/{key}"}) {
		t.Fatalf("new route was not discovered: %+v", handlers)
	}
}
