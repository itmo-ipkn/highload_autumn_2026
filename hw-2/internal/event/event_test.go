package event

import (
	"encoding/json"
	"errors"
	"math/rand/v2"
	"strings"
	"testing"
)

func TestUserIDsDisjoint(t *testing.T) {
	seen := map[string]string{}
	for _, p := range []string{"1", "2", "3"} {
		ids := UserIDs(p, 50)
		if len(ids) != 50 {
			t.Fatalf("producer %s: %d ids", p, len(ids))
		}
		for _, id := range ids {
			if !strings.HasPrefix(id, "u-"+p+"-") {
				t.Fatalf("id %s without producer prefix %s", id, p)
			}
			if other, ok := seen[id]; ok {
				t.Fatalf("id %s in producers %s and %s", id, other, p)
			}
			seen[id] = p
		}
	}
}

func TestGeneratorSeqWithoutGaps(t *testing.T) {
	g := NewGenerator("1", 7, 0, rand.New(rand.NewPCG(1, 2)))
	last := map[string]int64{}
	for range 1000 {
		e := g.Next()
		if e.Seq != last[e.UserID]+1 {
			t.Fatalf("user %s: seq %d after %d", e.UserID, e.Seq, last[e.UserID])
		}
		last[e.UserID] = e.Seq
		if e.Payload.Producer != "1" || !strings.HasPrefix(e.UserID, "u-1-") {
			t.Fatalf("event of another producer: %+v", e)
		}
	}
	for u, s := range last {
		if g.Seq(u) != s {
			t.Fatalf("Seq(%s) = %d, want %d", u, g.Seq(u), s)
		}
	}
	if len(last) != 7 {
		t.Fatalf("users used: %d", len(last))
	}
}

func TestGeneratorPoisonRate(t *testing.T) {
	g := NewGenerator("2", 10, 0.02, rand.New(rand.NewPCG(3, 4)))
	poison := 0
	const n = 20000
	for range n {
		if g.Next().Payload.Poison {
			poison++
		}
	}
	if poison < n*15/1000 || poison > n*25/1000 {
		t.Fatalf("poison %d of %d", poison, n)
	}

	g = NewGenerator("2", 10, 0, rand.New(rand.NewPCG(3, 4)))
	for range 1000 {
		if g.Next().Payload.Poison {
			t.Fatal("poison with rate 0")
		}
	}
}

func TestDecode(t *testing.T) {
	g := NewGenerator("3", 1, 0, rand.New(rand.NewPCG(5, 6)))
	e := g.Next()
	b := mustJSON(t, e)
	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.EventID != e.EventID || got.Seq != 1 || got.UserID != e.UserID {
		t.Fatalf("decoded %+v", got)
	}

	e.Payload.Poison = true
	if _, err := Decode(mustJSON(t, e)); !errors.Is(err, ErrPoison) {
		t.Fatalf("poison: %v", err)
	}
	if _, err := Decode([]byte("{")); err == nil {
		t.Fatal("broken json decoded")
	}
	if _, err := Decode([]byte(`{"event_id":"x","user_id":"u","seq":0}`)); err == nil {
		t.Fatal("seq 0 decoded")
	}
}

func mustJSON(t *testing.T, e Event) []byte {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
