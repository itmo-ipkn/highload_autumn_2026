// Package store - in-memory хранилище записей.
package store

import (
	"errors"
	"sync"
)

var ErrNotFound = errors.New("item not found")

type Item struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Store struct {
	mu     sync.RWMutex
	items  map[int64]Item
	nextID int64
}

func New() *Store {
	return &Store{items: make(map[int64]Item)}
}

func (s *Store) Create(it Item) Item {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	it.ID = s.nextID
	s.items[it.ID] = it
	return it
}

func (s *Store) Get(id int64) (Item, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	it, ok := s.items[id]
	if !ok {
		return Item{}, ErrNotFound
	}
	return it, nil
}

func (s *Store) Update(id int64, it Item) (Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[id]; !ok {
		return Item{}, ErrNotFound
	}
	it.ID = id
	s.items[id] = it
	return it, nil
}
