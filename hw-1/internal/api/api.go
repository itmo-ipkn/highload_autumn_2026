package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"highload-hw1/internal/items"
	"highload-hw1/internal/monitoring"
)

type API struct{ store *items.Store }

func New(store *items.Store, podID, datacenter string) http.Handler {
	api := &API{store: store}
	metrics := monitoring.New(podID, datacenter)
	business := chi.NewRouter()
	// Recoverer внутри мониторинга: перехваченная паника попадёт в метрики как 500.
	measured := business.With(metrics.Middleware, middleware.Recoverer)
	measured.Post("/items", api.create)
	measured.Get("/items/{id}", api.get)
	measured.Put("/items/{id}", api.update)
	handlers := monitoring.Catalog(business)

	router := chi.NewRouter()
	router.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	router.Handle("/metrics", metrics.Handler())
	router.Get("/handlers", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"handlers": handlers})
	})
	router.Mount("/", business)
	return router
}

func (a *API) create(w http.ResponseWriter, r *http.Request) {
	input, ok := decodeInput(w, r)
	if !ok {
		return
	}
	item := a.store.Create(input)
	w.Header().Set("Location", "/items/"+strconv.FormatInt(item.ID, 10))
	writeJSON(w, http.StatusCreated, item)
}

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	item, found := a.store.Get(id)
	if !found {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (a *API) update(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	input, ok := decodeInput(w, r)
	if !ok {
		return
	}
	item, found := a.store.Update(id, input)
	if !found {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func parseID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "id must be a positive integer")
		return 0, false
	}
	return id, true
}

func decodeInput(w http.ResponseWriter, r *http.Request) (items.Input, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input items.Input
	if err := decoder.Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return input, false
	}
	// После первого JSON не должно быть других данных.
	if err := decoder.Decode(new(any)); err != io.EOF {
		writeError(w, http.StatusBadRequest, "body must contain exactly one JSON object")
		return input, false
	}
	input.Name = strings.TrimSpace(input.Name)
	if input.Name == "" || len(input.Name) > 200 {
		writeError(w, http.StatusBadRequest, "name must contain 1 to 200 bytes")
		return input, false
	}
	return input, true
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// Ответы сериализуемы; ошибка записи не позволит отправить новый ответ.
	_ = json.NewEncoder(w).Encode(value)
}
