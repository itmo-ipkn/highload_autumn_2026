package item

import (
	"errors"
	"fmt"
	"sync"
)

const maxIDAttempts = 3

var (
	ErrNotFound = errors.New("item not found")
	ErrInvalid  = errors.New("invalid item")
)

type Item struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Store struct {
	items sync.Map
	newID func() (string, error)
}

func NewStore() *Store {
	return &Store{newID: newID}
}

func (s *Store) Create(name, value string) (Item, error) {
	if err := validateFields(name, value); err != nil {
		return Item{}, err
	}

	var lastErr error
	for range maxIDAttempts {
		id, err := s.newID()
		if err != nil {
			return Item{}, fmt.Errorf("create item: %w", err)
		}
		item := Item{ID: id, Name: name, Value: value}
		if _, loaded := s.items.LoadOrStore(id, item); !loaded {
			return item, nil
		}
		lastErr = fmt.Errorf("create item: generated id %s already exists", id)
	}
	if lastErr == nil {
		lastErr = errors.New("create item: unique id was not generated")
	}
	return Item{}, lastErr
}

func (s *Store) Get(id string) (Item, error) {
	value, ok := s.items.Load(id)
	if !ok {
		return Item{}, ErrNotFound
	}
	item, ok := value.(Item)
	if !ok {
		return Item{}, fmt.Errorf("get item: stored value for %s has type %T", id, value)
	}
	return item, nil
}

func (s *Store) Update(id, name, value string) (Item, error) {
	if id == "" {
		return Item{}, ErrInvalid
	}
	if err := validateFields(name, value); err != nil {
		return Item{}, err
	}
	if _, err := s.Get(id); err != nil {
		return Item{}, err
	}
	item := Item{ID: id, Name: name, Value: value}
	s.items.Store(id, item)
	return item, nil
}

func validateFields(name, value string) error {
	if name == "" || value == "" {
		return ErrInvalid
	}
	return nil
}
