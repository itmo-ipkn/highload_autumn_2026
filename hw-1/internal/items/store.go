package items

import "sync"

// Возвращаем копии: менять данные хранилища можно только через его методы.
type Item struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Input struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Данные живут в памяти одного экземпляра приложения.
type Store struct {
	mu     sync.RWMutex
	nextID int64
	items  map[int64]Item
}

func NewStore() *Store {
	return &Store{items: make(map[int64]Item)}
}

func (s *Store) Create(input Input) Item {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	item := Item{ID: s.nextID, Name: input.Name, Value: input.Value}
	s.items[item.ID] = item
	return item
}

func (s *Store) Get(id int64) (Item, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, ok := s.items[id]
	return item, ok
}

func (s *Store) Update(id int64, input Input) (Item, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Проверка и замена выполняются под одной блокировкой.
	if _, ok := s.items[id]; !ok {
		return Item{}, false
	}
	item := Item{ID: id, Name: input.Name, Value: input.Value}
	s.items[id] = item
	return item, true
}
