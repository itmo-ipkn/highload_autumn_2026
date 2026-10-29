package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"mime"
	"net/http"

	"highload/lab1/internal/item"
)

type itemRequest struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func (a *API) create(w http.ResponseWriter, r *http.Request) {
	req, ok := a.readItem(w, r)
	if !ok {
		return
	}
	created, err := a.store.Create(req.Name, req.Value)
	if err != nil {
		a.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	found, err := a.store.Get(r.PathValue("id"))
	if err != nil {
		a.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, found)
}

func (a *API) update(w http.ResponseWriter, r *http.Request) {
	req, ok := a.readItem(w, r)
	if !ok {
		return
	}
	updated, err := a.store.Update(r.PathValue("id"), req.Name, req.Value)
	if err != nil {
		a.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (a *API) readItem(w http.ResponseWriter, r *http.Request) (itemRequest, bool) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusBadRequest, "invalid request")
		return itemRequest{}, false
	}

	reader := http.MaxBytesReader(w, r.Body, a.maxBodyBytes)
	defer reader.Close()
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()

	var req itemRequest
	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return itemRequest{}, false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid request")
		return itemRequest{}, false
	}
	if req.Name == "" || req.Value == "" {
		writeError(w, http.StatusBadRequest, "invalid request")
		return itemRequest{}, false
	}
	return req, true
}

func (a *API) writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, item.ErrInvalid):
		writeError(w, http.StatusBadRequest, "invalid request")
	case errors.Is(err, item.ErrNotFound):
		writeError(w, http.StatusNotFound, "item not found")
	default:
		log.Printf("item storage: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}
