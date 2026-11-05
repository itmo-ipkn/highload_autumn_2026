package storage

import (
	"my-service/internal/model"
	"sync"
	"time"
)

type InMemoryStorage struct {
	mu     sync.RWMutex
	items  map[int]model.Item
	nextID int
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		items:  make(map[int]model.Item),
		nextID: 1,
	}
}

func (s *InMemoryStorage) Create(data any) model.Item {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID
	s.nextID++
	item := model.Item{
		ID:        id,
		Data:      data,
		UpdatedAt: time.Now(),
	}
	s.items[id] = item
	return item
}

func (s *InMemoryStorage) Get(id int) (model.Item, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, exists := s.items[id]
	return item, exists
}

func (s *InMemoryStorage) Update(id int, data interface{}) (model.Item, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.items[id]; !exists {
		return model.Item{}, false
	}
	item := model.Item{
		ID:        id,
		Data:      data,
		UpdatedAt: time.Now(),
	}
	s.items[id] = item
	return item, true
}
