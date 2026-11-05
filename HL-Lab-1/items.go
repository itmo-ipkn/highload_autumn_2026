package main

import (
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"sync"

	"github.com/google/uuid"
)

type Item struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

type itemInput struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

const maxItemBodyBytes = 1 << 20

type Store struct {
	mu    sync.RWMutex
	items map[string]Item
}

func NewStore() *Store { return &Store{items: make(map[string]Item)} }

func (s *Store) create(item Item) (Item, error) {
	id, err := newID()
	if err != nil {
		return Item{}, err
	}
	item.ID = id
	s.mu.Lock()
	s.items[id] = item
	s.mu.Unlock()
	return item, nil
}

func (s *Store) get(id string) (Item, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, ok := s.items[id]
	return item, ok
}

func (s *Store) update(id string, item Item) (Item, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[id]; !ok {
		return Item{}, false
	}
	item.ID = id
	s.items[id] = item
	return item, true
}

func newID() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func itemRoutes(store *Store, failureProbability float64) []route {
	return []route{
		{handlerInfo{"POST", "/items"}, createItemHandler(store)},
		{handlerInfo{"GET", "/items/{id}"}, getItemHandler(store)},
		{handlerInfo{"PUT", "/items/{id}"}, updateItemHandler(store, failureProbability)},
	}
}

func createItemHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		input, err := decodeItem(w, r)
		if err != nil {
			writeItemDecodeError(w, err)
			return
		}
		created, err := store.create(Item{Name: input.Name, Value: input.Value})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not generate item ID")
			return
		}
		writeJSON(w, http.StatusCreated, created)
	}
}

func getItemHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		item, ok := store.get(r.PathValue("id"))
		if !ok {
			writeError(w, http.StatusNotFound, "item not found")
			return
		}
		writeJSON(w, http.StatusOK, item)
	}
}

func updateItemHandler(store *Store, failureProbability float64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if rand.Float64() < failureProbability {
			writeError(w, http.StatusInternalServerError, "simulated server failure")
			return
		}
		input, err := decodeItem(w, r)
		if err != nil {
			writeItemDecodeError(w, err)
			return
		}
		updated, ok := store.update(r.PathValue("id"), Item{Name: input.Name, Value: input.Value})
		if !ok {
			writeError(w, http.StatusNotFound, "item not found")
			return
		}
		writeJSON(w, http.StatusOK, updated)
	}
}

func decodeItem(w http.ResponseWriter, r *http.Request) (itemInput, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxItemBodyBytes)
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var item *itemInput
	if err := decoder.Decode(&item); err != nil {
		return itemInput{}, err
	}
	if item == nil {
		return itemInput{}, errors.New("JSON object required")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return itemInput{}, errors.New("multiple JSON values")
		}
		return itemInput{}, err
	}
	return *item, nil
}

func writeItemDecodeError(w http.ResponseWriter, err error) {
	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	writeError(w, http.StatusBadRequest, "invalid JSON body")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
