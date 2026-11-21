package handlers

import (
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"hw_task1/internal/storage"
)

type Router interface {
	Handle(pattern string, handler http.Handler)
}

type Items struct {
	store         *storage.Storage
	errorRate     float64
	medianLatency time.Duration
}

func NewItems(store *storage.Storage, errorRate float64, medianLatency time.Duration) *Items {
	return &Items{store: store, errorRate: errorRate, medianLatency: medianLatency}
}

func (h *Items) Register(r Router) {
	r.Handle("POST /items", h.withChaos(h.create))
	r.Handle("GET /items/{id}", h.withChaos(h.get))
	r.Handle("PUT /items/{id}", h.withChaos(h.update))
}

func Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type itemRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

func (h *Items) create(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeItem(w, r)
	if !ok {
		return
	}
	item := h.store.Create(storage.Item{Name: req.Name, Description: req.Description})
	writeJSON(w, http.StatusCreated, item)
}

func (h *Items) get(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	item, err := h.store.Get(id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (h *Items) update(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	req, ok := decodeItem(w, r)
	if !ok {
		return
	}
	item, err := h.store.Update(id, storage.Item{Name: req.Name, Description: req.Description})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

const (
	latencySigma = 0.8
	maxLatency   = 2 * time.Second
)

func (h *Items) withChaos(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.medianLatency > 0 {
			d := time.Duration(float64(h.medianLatency) * math.Exp(latencySigma*rand.NormFloat64()))
			select {
			case <-time.After(min(d, maxLatency)):
			case <-r.Context().Done():
				return
			}
		}
		if h.errorRate > 0 && rand.Float64() < h.errorRate {
			writeError(w, http.StatusInternalServerError, "injected failure")
			return
		}
		next(w, r)
	})
}

const maxBodyBytes = 1 << 20

func decodeItem(w http.ResponseWriter, r *http.Request) (itemRequest, bool) {
	var req itemRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return req, false
	}
	if strings.TrimSpace(req.Name) == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return req, false
	}
	return req, true
}

func parseID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "id must be a positive integer")
		return 0, false
	}
	return id, true
}

func writeStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, err.Error())
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("write response", "err", err)
	}
}
