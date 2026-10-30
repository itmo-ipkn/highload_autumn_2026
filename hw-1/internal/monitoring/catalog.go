package monitoring

import (
	"net/http"
	"sort"

	"github.com/go-chi/chi/v5"
)

type Handler struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

// Список ручек берём из роутера.
func Catalog(routes chi.Routes) []Handler {
	handlers := make([]Handler, 0)
	err := chi.Walk(routes, func(method, path string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		handlers = append(handlers, Handler{Method: method, Path: path})
		return nil
	})
	if err != nil {
		panic(err)
	} // Наш callback всегда возвращает nil.
	sort.Slice(handlers, func(i, j int) bool {
		if handlers[i].Path != handlers[j].Path {
			return handlers[i].Path < handlers[j].Path
		}
		return handlers[i].Method < handlers[j].Method
	})
	return handlers
}
