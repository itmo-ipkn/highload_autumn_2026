package items_test

import (
	"sync"
	"testing"

	"highload-hw1/internal/items"
)

func TestConcurrentAccess(t *testing.T) {
	store := items.NewStore()
	shared := store.Create(items.Input{Name: "shared"})
	const workers = 100
	ids := make(chan int64, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			item := store.Create(items.Input{Name: "new"})
			ids <- item.ID
			if _, ok := store.Get(item.ID); !ok {
				t.Error("created item disappeared")
			}
			if _, ok := store.Update(shared.ID, items.Input{Name: "updated"}); !ok {
				t.Error("shared item disappeared")
			}
			store.Get(shared.ID)
		}()
	}
	wg.Wait()
	close(ids)
	seen := make(map[int64]bool)
	for id := range ids {
		if seen[id] {
			t.Errorf("duplicate ID %d", id)
		}
		seen[id] = true
	}
	if len(seen) != workers {
		t.Fatalf("got %d unique IDs", len(seen))
	}
}

func TestStoresAreIndependent(t *testing.T) {
	first, second := items.NewStore(), items.NewStore()
	item := first.Create(items.Input{Name: "only in first"})
	if _, ok := second.Get(item.ID); ok {
		t.Fatal("instances share state")
	}
}
