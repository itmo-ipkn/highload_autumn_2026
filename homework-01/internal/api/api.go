package api

import (
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/Fadet/highload-hw1/internal/store"
)

type Router interface {
	Handle(pattern string, handler http.Handler)
}

type ItemRequest struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

const maxLatency = 2 * time.Second

type Options struct {
	FailureRate float64
	LatencyMean time.Duration
}

type Handlers struct {
	store *store.Store
	opts  Options
}

func New(s *store.Store, opts Options) *Handlers {
	return &Handlers{store: s, opts: opts}
}

func (h *Handlers) Register(r Router) {
	r.Handle("POST /items", h.chaos(h.createItem))
	r.Handle("GET /items/{id}", h.chaos(h.getItem))
	r.Handle("PUT /items/{id}", h.chaos(h.updateItem))
}

func (h *Handlers) chaos(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.opts.LatencyMean > 0 {
			delay := min(time.Duration(rand.ExpFloat64()*float64(h.opts.LatencyMean)), maxLatency)
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		if h.opts.FailureRate > 0 && rand.Float64() < h.opts.FailureRate {
			writeError(w, http.StatusInternalServerError, "injected failure")
			return
		}
		next(w, r)
	})
}

func Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handlers) createItem(w http.ResponseWriter, r *http.Request) {
	req, ok := decodeItemRequest(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusCreated, h.store.Create(req.Name, req.Value))
}

func (h *Handlers) getItem(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	item, err := h.store.Get(id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (h *Handlers) updateItem(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	req, ok := decodeItemRequest(w, r)
	if !ok {
		return
	}
	item, err := h.store.Update(id, req.Name, req.Value)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func parseID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "id must be a positive integer")
		return 0, false
	}
	return id, true
}

func decodeItemRequest(w http.ResponseWriter, r *http.Request) (ItemRequest, bool) {
	var req ItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return req, false
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return req, false
	}
	return req, true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
