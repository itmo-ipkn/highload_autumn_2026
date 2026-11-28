package httpapi

import (
	"net/http"
	"sync/atomic"
)

type Health struct {
	ready atomic.Bool
}

type healthBody struct {
	Status string `json:"status"`
}

func NewHealth() *Health {
	return &Health{}
}

func (h *Health) SetReady(ready bool) {
	h.ready.Store(ready)
}

func (h *Health) Ready() bool {
	return h.ready.Load()
}

func (h *Health) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if !h.Ready() {
		writeError(w, http.StatusServiceUnavailable, "not ready")
		return
	}
	writeJSON(w, http.StatusOK, healthBody{Status: "ok"})
}
