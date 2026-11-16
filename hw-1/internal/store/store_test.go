package store

import (
	"errors"
	"sync"
	"testing"
)

func TestCreateGetUpdate(t *testing.T) {
	s := New()
	a := s.Create(Item{Name: "a"})
	b := s.Create(Item{Name: "b"})
	if a.ID != 1 || b.ID != 2 {
		t.Fatalf("ids: got %d, %d", a.ID, b.ID)
	}

	got, err := s.Get(a.ID)
	if err != nil || got != a {
		t.Fatalf("get: got %+v, %v", got, err)
	}

	upd, err := s.Update(a.ID, Item{ID: 100, Name: "a2"})
	if err != nil || upd.ID != a.ID || upd.Name != "a2" {
		t.Fatalf("update: got %+v, %v", upd, err)
	}
}

func TestNotFound(t *testing.T) {
	s := New()
	if _, err := s.Get(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get: got %v", err)
	}
	if _, err := s.Update(1, Item{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update: got %v", err)
	}
}

func TestConcurrentCreate(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() { s.Create(Item{}) })
	}
	wg.Wait()
	if _, err := s.Get(100); err != nil {
		t.Fatal(err)
	}
}
