package item

import (
	"errors"
	"fmt"
	"regexp"
	"sync"
	"testing"
)

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestCreateGetAndUpdate(t *testing.T) {
	t.Parallel()

	store := NewStore()
	created, err := store.Create("example", "some value")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !uuidV4.MatchString(created.ID) {
		t.Fatalf("id %q is not a uuid v4", created.ID)
	}
	if created.Name != "example" || created.Value != "some value" {
		t.Fatalf("created item = %+v", created)
	}

	got, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != created {
		t.Fatalf("get = %+v, created %+v", got, created)
	}

	created.Name = "mutated by caller"
	stored, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("get after caller mutation: %v", err)
	}
	if stored.Name != "example" {
		t.Fatalf("caller mutation changed stored name to %q", stored.Name)
	}

	updated, err := store.Update(created.ID, "renamed", "new value")
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.ID != stored.ID || updated.Name != "renamed" || updated.Value != "new value" {
		t.Fatalf("update = %+v", updated)
	}
	if stored.Name != "example" || stored.Value != "some value" {
		t.Fatalf("update changed the previously returned value: %+v", stored)
	}

	got, err = store.Get(created.ID)
	if err != nil {
		t.Fatalf("get updated: %v", err)
	}
	if got != updated {
		t.Fatalf("stored %+v, updated %+v", got, updated)
	}
}

func TestCreateIDsAreUnique(t *testing.T) {
	t.Parallel()

	store := NewStore()
	seen := make(map[string]struct{}, 100)
	for range 100 {
		created, err := store.Create("name", "value")
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, ok := seen[created.ID]; ok {
			t.Fatalf("duplicate id %s", created.ID)
		}
		seen[created.ID] = struct{}{}
	}
}

func TestMissingItemAndInvalidFields(t *testing.T) {
	t.Parallel()

	store := NewStore()
	if _, err := store.Get("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing: %v", err)
	}
	if _, err := store.Update("missing", "name", "value"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update missing: %v", err)
	}
	if countItems(store) != 0 {
		t.Fatal("update of a missing id stored an item")
	}

	for _, fields := range []struct{ name, value string }{
		{name: "", value: "value"},
		{name: "name", value: ""},
	} {
		if _, err := store.Create(fields.name, fields.value); !errors.Is(err, ErrInvalid) {
			t.Fatalf("create %q/%q: %v", fields.name, fields.value, err)
		}
		if _, err := store.Update("id", fields.name, fields.value); !errors.Is(err, ErrInvalid) {
			t.Fatalf("update %q/%q: %v", fields.name, fields.value, err)
		}
	}
	if _, err := store.Update("", "name", "value"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("update empty id: %v", err)
	}
	if countItems(store) != 0 {
		t.Fatal("invalid operations stored an item")
	}
}

func TestStoresAreIndependent(t *testing.T) {
	t.Parallel()

	first := NewStore()
	second := NewStore()
	created, err := first.Create("name", "value")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := second.Get(created.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second store returned %v", err)
	}
	if _, err := second.Update(created.ID, "other", "value"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second store update returned %v", err)
	}
}

func TestCreateRetriesCollidingIDs(t *testing.T) {
	t.Parallel()

	store := NewStore()
	store.items.Store("dup", Item{ID: "dup", Name: "kept", Value: "kept"})
	calls := 0
	store.newID = func() (string, error) {
		calls++
		if calls < 3 {
			return "dup", nil
		}
		return "fresh", nil
	}

	created, err := store.Create("name", "value")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.ID != "fresh" {
		t.Fatalf("id = %s", created.ID)
	}
	kept, err := store.Get("dup")
	if err != nil {
		t.Fatalf("get original: %v", err)
	}
	if kept.Name != "kept" {
		t.Fatalf("collision overwrote the original item: %+v", kept)
	}
}

func TestCreateStopsAfterRepeatedCollisions(t *testing.T) {
	t.Parallel()

	store := NewStore()
	store.items.Store("dup", Item{ID: "dup", Name: "kept", Value: "kept"})
	store.newID = func() (string, error) {
		return "dup", nil
	}
	if _, err := store.Create("name", "value"); err == nil {
		t.Fatal("expected collision error")
	}
	if countItems(store) != 1 {
		t.Fatalf("item count = %d", countItems(store))
	}
}

func TestCreateIDFailureStoresNothing(t *testing.T) {
	t.Parallel()

	store := NewStore()
	store.newID = func() (string, error) {
		return "", errors.New("entropy unavailable")
	}
	_, err := store.Create("name", "value")
	if err == nil || errors.Is(err, ErrInvalid) {
		t.Fatalf("create: %v", err)
	}
	if countItems(store) != 0 {
		t.Fatal("failed id generation stored an item")
	}
}

func TestConcurrentCreateAndUpdate(t *testing.T) {
	t.Parallel()

	store := NewStore()
	seed, err := store.Create("seed", "value")
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := store.Create(fmt.Sprintf("name-%d", i), "value"); err != nil {
				t.Errorf("create: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := store.Update(seed.ID, "seed", fmt.Sprintf("value-%d", i)); err != nil {
				t.Errorf("update: %v", err)
			}
		}()
	}
	wg.Wait()

	got, err := store.Get(seed.ID)
	if err != nil {
		t.Fatalf("get seed: %v", err)
	}
	if got.ID != seed.ID || got.Name != "seed" {
		t.Fatalf("seed changed identity: %+v", got)
	}
	if countItems(store) != 33 {
		t.Fatalf("item count = %d, want 33", countItems(store))
	}
}

func countItems(store *Store) int {
	count := 0
	store.items.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
