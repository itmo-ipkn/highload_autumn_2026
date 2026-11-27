package generator

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"highload/hw2/producer/internal/event"
)

type recordingPublisher struct {
	mu     sync.Mutex
	events []event.Event
}

func (p *recordingPublisher) Publish(_ context.Context, e event.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
	return nil
}

func TestRunKeepsSequenceWithoutGaps(t *testing.T) {
	pub := new(recordingPublisher)
	g, err := New(pub, []string{"user-1"},
		WithUnlimitedRate(),
		WithWorkers(4),
		WithMaxEvents(100),
		WithPoisonRate(0),
		WithSeed(42),
		WithClock(func() time.Time { return time.Unix(1, 0) }),
		WithIDGenerator(uuid.New),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if got, want := len(pub.events), 100; got != want {
		t.Fatalf("published %d events, want %d", got, want)
	}
	for i, e := range pub.events {
		if got, want := e.Seq, uint64(i+1); got != want {
			t.Fatalf("event %d has seq %d, want %d", i, got, want)
		}
		if err := e.Validate(); err != nil {
			t.Fatalf("event %d is invalid: %v", i, err)
		}
	}
}

func TestBuildPayloadMarksPoisonEvent(t *testing.T) {
	payload, err := buildPayload(rand.New(rand.NewPCG(1, 2)), event.TypePurchase, true)
	if err != nil {
		t.Fatal(err)
	}

	var fields map[string]any
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	if got, ok := fields[event.PoisonField].(bool); !ok || !got {
		t.Fatalf("payload poison field = %v, want true", fields[event.PoisonField])
	}
}

func TestUserIDsAreStableAndUnique(t *testing.T) {
	got := UserIDs("p1", "run", 3)
	want := []string{"pp1-run-u001", "pp1-run-u002", "pp1-run-u003"}
	if len(got) != len(want) {
		t.Fatalf("got %d user IDs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("user ID %d = %q, want %q", i, got[i], want[i])
		}
	}
	if sorted := append([]string(nil), got...); !sort.StringsAreSorted(sorted) {
		t.Fatal("user IDs are not sortable by their sequence suffix")
	}
}
