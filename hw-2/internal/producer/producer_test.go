package producer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/event"
)

func TestParseKeyMode(t *testing.T) {
	for _, s := range []string{"user_id", "none"} {
		if _, err := ParseKeyMode(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	if _, err := ParseKeyMode("random"); err == nil {
		t.Fatal("random accepted")
	}
}

func TestRecordKey(t *testing.T) {
	e := event.NewGenerator("1", 3, 0, rand.New(rand.NewPCG(1, 1))).Next()
	r, err := Record(e, KeyUserID)
	if err != nil {
		t.Fatal(err)
	}
	if string(r.Key) != e.UserID {
		t.Fatalf("key %q, want %q", r.Key, e.UserID)
	}
	got, err := event.Decode(r.Value)
	if err != nil || got.EventID != e.EventID {
		t.Fatalf("value %s: %v", r.Value, err)
	}

	r, err = Record(e, KeyNone)
	if err != nil {
		t.Fatal(err)
	}
	if r.Key != nil {
		t.Fatalf("key %q with KEY_MODE=none", r.Key)
	}
}

type fakeClient struct {
	mu      sync.Mutex
	records []*kgo.Record
	fail    bool
	flushed bool
}

func (f *fakeClient) Produce(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	f.mu.Lock()
	f.records = append(f.records, r)
	f.mu.Unlock()
	if f.fail {
		promise(r, errors.New("boom"))
		return
	}
	promise(r, nil)
}

func (f *fakeClient) Flush(context.Context) error { f.flushed = true; return nil }

func (f *fakeClient) BufferedProduceRecords() int64 { return 0 }

func TestRunCountsAndFlushes(t *testing.T) {
	for _, fail := range []bool{false, true} {
		cl := &fakeClient{fail: fail}
		gen := event.NewGenerator("1", 5, 0.5, rand.New(rand.NewPCG(7, 7)))
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		var st Stats
		Run(ctx, Config{Rate: 200, KeyMode: KeyUserID, FlushTimeout: time.Second}, gen, cl,
			NewMetrics(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)), &st)
		cancel()

		if !cl.flushed {
			t.Fatal("not flushed")
		}
		n := int64(len(cl.records))
		if n == 0 || st.Generated.Load() != n {
			t.Fatalf("generated %d, produced %d", st.Generated.Load(), n)
		}
		if fail && (st.Failed.Load() != n || st.Acked.Load() != 0) {
			t.Fatalf("fail: acked %d failed %d of %d", st.Acked.Load(), st.Failed.Load(), n)
		}
		if !fail && (st.Acked.Load() != n || st.Failed.Load() != 0) {
			t.Fatalf("ok: acked %d failed %d of %d", st.Acked.Load(), st.Failed.Load(), n)
		}
	}
}
