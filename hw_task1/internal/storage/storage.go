package storage

import (
	"errors"
	"sync"
)

var ErrNotFound = errors.New("item not found")

type Item struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type Storage struct {
	mu     sync.RWMutex
	items  map[int64]Item
	nextID int64
}

func New() *Storage {
	return &Storage{items: make(map[int64]Item), nextID: 1}
}

func (s *Storage) Create(item Item) Item {
	s.mu.Lock()
	defer s.mu.Unlock()

	item.ID = s.nextID
	s.nextID++
	s.items[item.ID] = item
	return item
}

func (s *Storage) Get(id int64) (Item, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, ok := s.items[id]
	if !ok {
		return Item{}, ErrNotFound
	}
	return item, nil
}

func (s *Storage) Update(id int64, item Item) (Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.items[id]; !ok {
		return Item{}, ErrNotFound
	}
	item.ID = id
	s.items[id] = item
	return item, nil
}
