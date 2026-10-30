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

func (s *Store) Create(name, value string) Item {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextID++
	item := Item{ID: s.nextID, Name: name, Value: value}
	s.items[item.ID] = item
	return item
}

func (s *Store) Get(id int64) (Item, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, ok := s.items[id]
	if !ok {
		return Item{}, ErrNotFound
	}
	return item, nil
}

func (s *Store) Update(id int64, name, value string) (Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.items[id]; !ok {
		return Item{}, ErrNotFound
	}
	item := Item{ID: id, Name: name, Value: value}
	s.items[id] = item
	return item, nil
}
