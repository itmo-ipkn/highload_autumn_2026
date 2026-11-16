package metrics

import (
	"encoding/json"
	"net/http"
	"sort"

	"github.com/go-chi/chi/v5"
)

type Route struct {
	Method string `json:"method"`
	Route  string `json:"route"`
}

// Routes возвращает ручки роутера, отсортированные по пути и методу.
func Routes(r chi.Routes) ([]Route, error) {
	var out []Route
	err := chi.Walk(r, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		out = append(out, Route{Method: method, Route: route})
		return nil
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].Route != out[j].Route {
			return out[i].Route < out[j].Route
		}
		return out[i].Method < out[j].Method
	})
	return out, err
}

// HandlersHandler отдаёт список ручек для GET /handlers.
func HandlersHandler(r chi.Routes) (http.Handler, error) {
	routes, err := Routes(r)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(map[string][]Route{"handlers": routes})
	if err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}), nil
}
