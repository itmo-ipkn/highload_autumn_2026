package app

import (
	"encoding/json"
	"net/http"
	"strconv"
)

func (a *Application) createItem(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Simulate-Error") == "500" {
		panic("simulated panic")
	}
	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	item := a.storage.Create(body)
	writeResponse(w, http.StatusCreated, item)
}

func (a *Application) getItem(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Simulate-Error") == "500" {
		panic("simulated panic")
	}
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}
	item, ok := a.storage.Get(id)
	if !ok {
		http.Error(w, "Item not found", http.StatusNotFound)
		return
	}
	writeResponse(w, http.StatusOK, item)
}

func (a *Application) updateItem(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Simulate-Error") == "500" {
		panic("simulated panic")
	}
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}
	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	item, ok := a.storage.Update(id, body)
	if !ok {
		http.Error(w, "Item not found", http.StatusNotFound)
		return
	}
	writeResponse(w, http.StatusOK, item)
}

func (a *Application) health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func (a *Application) handlers(w http.ResponseWriter, r *http.Request) {
	writeResponse(w, http.StatusOK, businessHandlers)
}
