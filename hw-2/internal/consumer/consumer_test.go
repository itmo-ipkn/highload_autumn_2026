package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/event"
	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/sink"
)

func rec(p int32, off int64) *kgo.Record {
	return &kgo.Record{Topic: "events", Partition: p, Offset: off, LeaderEpoch: 1}
}

func TestOffsets(t *testing.T) {
	o := NewOffsets()
	o.Mark(rec(0, 10))
	o.Mark(rec(0, 11))
	o.Mark(rec(1, 5))
	o.Mark(rec(0, 3)) // повтор старой записи не откатывает оффсет

	got := o.Pending()
	if got[0].Offset != 12 || got[1].Offset != 6 || len(got) != 2 {
		t.Fatalf("pending %v", got)
	}
	if only := o.Pending(1, 2); len(only) != 1 || only[1].Offset != 6 {
		t.Fatalf("pending(1,2) %v", only)
	}

	// пока коммит летел, партиция 0 продвинулась - её оставляем
	o.Mark(rec(0, 12))
	o.Committed(got)
	left := o.Pending()
	if len(left) != 1 || left[0].Offset != 13 {
		t.Fatalf("after commit %v", left)
	}

	o.Drop(0)
	if len(o.Pending()) != 0 {
		t.Fatalf("after drop %v", o.Pending())
	}
}

func TestDLTRecord(t *testing.T) {
	src := &kgo.Record{Topic: "events", Partition: 2, Offset: 42, Key: []byte("u-1-0001"), Value: []byte(`{"x":1}`)}
	r := DLTRecord(src, "events.dlt", event.ErrPoison, 3, "consumer-1")
	if r.Topic != "events.dlt" || string(r.Key) != "u-1-0001" || string(r.Value) != `{"x":1}` {
		t.Fatalf("record %+v", r)
	}
	want := map[string]string{
		HeaderError:             "poison event",
		HeaderOriginalTopic:     "events",
		HeaderOriginalPartition: "2",
		HeaderOriginalOffset:    "42",
		HeaderAttempts:          "3",
		HeaderConsumer:          "consumer-1",
	}
	for k, v := range want {
		if got := Header(r, k); got != v {
			t.Errorf("header %s = %q, want %q", k, got, v)
		}
	}
}

type fakeSink struct {
	rows map[string]sink.Row
	fail int
	dups int
}

func (s *fakeSink) Insert(_ context.Context, r sink.Row) (bool, error) {
	if s.fail > 0 {
		s.fail--
		return false, errors.New("connection refused")
	}
	if _, ok := s.rows[r.EventID]; ok {
		s.dups++
		return true, nil
	}
	s.rows[r.EventID] = r
	return false, nil
}

type fakeDLT struct{ records []*kgo.Record }

func (d *fakeDLT) Publish(_ context.Context, r *kgo.Record) error {
	d.records = append(d.records, r)
	return nil
}

func newProc(s Sink, d DLT) *Processor {
	return &Processor{
		ConsumerID: "consumer-1", MaxAttempts: 3, Sink: s, DLT: d, DLTTopic: "events.dlt",
		Metrics: NewMetrics(prometheus.NewRegistry()),
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func eventRecord(t *testing.T, poison bool, off int64) *kgo.Record {
	t.Helper()
	e := event.NewGenerator("1", 1, 0, rand.New(rand.NewPCG(1, 1))).Next()
	e.Payload.Poison = poison
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: "events", Partition: 1, Offset: off, Key: []byte(e.UserID), Value: b}
}

func TestHandle(t *testing.T) {
	s := &fakeSink{rows: map[string]sink.Row{}, fail: 2}
	d := &fakeDLT{}
	p := newProc(s, d)
	ctx := context.Background()

	ok := eventRecord(t, false, 7)
	if out, err := p.Handle(ctx, ok); err != nil || out != OutcomeOK {
		t.Fatalf("ok: %s %v", out, err)
	}
	if len(s.rows) != 1 {
		t.Fatalf("rows %d", len(s.rows))
	}
	for _, r := range s.rows {
		if r.Partition != 1 || r.Offset != 7 || r.ConsumerID != "consumer-1" || r.Seq != 1 || r.ProducerID != "1" {
			t.Fatalf("row %+v", r)
		}
	}

	if out, err := p.Handle(ctx, ok); err != nil || out != OutcomeDuplicate || s.dups != 1 {
		t.Fatalf("dup: %s %v dups=%d", out, err, s.dups)
	}

	poison := eventRecord(t, true, 8)
	if out, err := p.Handle(ctx, poison); err != nil || out != OutcomeDLT {
		t.Fatalf("poison: %s %v", out, err)
	}
	if len(d.records) != 1 || Header(d.records[0], HeaderAttempts) != "3" || len(s.rows) != 1 {
		t.Fatalf("dlt %d records, rows %d", len(d.records), len(s.rows))
	}

	broken := &kgo.Record{Topic: "events", Partition: 0, Offset: 9, Value: []byte("not json")}
	if out, err := p.Handle(ctx, broken); err != nil || out != OutcomeDLT {
		t.Fatalf("broken: %s %v", out, err)
	}
}

func TestHandleCanceled(t *testing.T) {
	s := &fakeSink{rows: map[string]sink.Row{}, fail: 1 << 30}
	p := newProc(s, &fakeDLT{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Handle(ctx, eventRecord(t, false, 1)); err == nil {
		t.Fatal("handled with canceled ctx and broken sink")
	}
}

func TestInitEvents(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewMetrics(reg).InitEvents([]int32{0, 1, 2})

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, mf := range mfs {
		if mf.GetName() != "consumer_events_total" {
			continue
		}
		for _, s := range mf.GetMetric() {
			l := map[string]string{}
			for _, lp := range s.GetLabel() {
				l[lp.GetName()] = lp.GetValue()
			}
			got["p"+l["partition"]+" "+l["result"]] = s.GetCounter().GetValue()
		}
	}
	if len(got) != 9 {
		t.Fatalf("series %v", got)
	}
	for _, p := range []string{"0", "1", "2"} {
		for _, o := range []Outcome{OutcomeOK, OutcomeDuplicate, OutcomeDLT} {
			if v, ok := got["p"+p+" "+string(o)]; !ok || v != 0 {
				t.Errorf("p%s %s = %v, present %v", p, o, v, ok)
			}
		}
	}
}
