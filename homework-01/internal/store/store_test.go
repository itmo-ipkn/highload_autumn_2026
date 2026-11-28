package store

import (
	"errors"
	"sync"
	"testing"
)

func TestCreateGetUpdate(t *testing.T) {
	s := New()

	created := s.Create("a", "1")
	if created.ID != 1 {
		t.Fatalf("expected id 1, got %d", created.ID)
	}

	got, err := s.Get(created.ID)
	if err != nil || got != created {
		t.Fatalf("Get = %+v, %v; want %+v", got, err, created)
	}

	updated, err := s.Update(created.ID, "b", "2")
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got, _ := s.Get(created.ID); got != updated || got.Name != "b" {
		t.Fatalf("after update Get = %+v; want %+v", got, updated)
	}
}

func TestNotFound(t *testing.T) {
	s := New()
	if _, err := s.Get(42); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get: want ErrNotFound, got %v", err)
	}
	if _, err := s.Update(42, "x", "y"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Update: want ErrNotFound, got %v", err)
	}
}

func TestConcurrentCreateUniqueIDs(t *testing.T) {
	s := New()
	const n = 100

	var wg sync.WaitGroup
	ids := make(chan int64, n)
	for range n {
		wg.Go(func() { ids <- s.Create("x", "y").ID })
	}
	wg.Wait()
	close(ids)

	seen := make(map[int64]bool)
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = true
	}
	if len(seen) != n {
		t.Fatalf("expected %d ids, got %d", n, len(seen))
	}
}
