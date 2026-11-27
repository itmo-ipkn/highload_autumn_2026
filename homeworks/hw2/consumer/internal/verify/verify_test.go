package verify

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"highload/hw2/consumer/internal/event"
)

func testEvent(user string, seq uint64, payload string) event.Event {
	return event.Event{EventID: uuid.New(), UserID: user, Seq: seq, Type: event.TypePurchase,
		ProducedAt: time.Unix(1, 0).UTC(), Payload: json.RawMessage(payload)}
}

func inSink(events ...event.Event) sinkSnapshot {
	s := sinkSnapshot{events: make(map[uuid.UUID]sequence)}
	for _, e := range events {
		s.events[e.EventID] = sequence{e.UserID, e.Seq}
	}
	return s
}

func TestCompareRedeliveryAndPayloadRouting(t *testing.T) {
	var source, dlt, normal []event.Event
	for i, payload := range []string{`"text"`, `[{"poison":true}]`, `{}`, `false`, `null`, `1e400`, `{"poison":"true"}`, `{"poison":false}`} {
		e := testEvent("a", uint64(i+1), payload)
		source = append(source, e, e)
		normal = append(normal, e)
	}
	poison := testEvent("b", 1, `{"poison": true}`)
	source = append(source, poison, poison)
	dlt = append(dlt, poison, poison)
	sink := inSink(normal...)
	sink.duplicates = 3
	r := compare(source, dlt, sink)
	if !r.Passed() || r.SinkEvents != int64(len(normal)) || r.DuplicateDeliveries != 3 || r.DLTEvents != 2 || r.PoisonEvents != 1 || r.UniqueDLTEvents != 1 || r.DuplicateDLTEvents != 1 {
		t.Fatalf("unexpected redelivery result: %+v", r)
	}
}

func TestCompareDistinctIDsForSameSequence(t *testing.T) {
	a, b := testEvent("user", 1, `{}`), testEvent("user", 1, `42`)
	for _, sink := range []sinkSnapshot{inSink(a, b), inSink(a)} {
		r := compare([]event.Event{a, b, b}, nil, sink)
		if r.Passed() || len(r.SequenceConflicts) != 1 || !strings.Contains(r.SequenceConflicts[0], a.EventID.String()) || !strings.Contains(r.SequenceConflicts[0], b.EventID.String()) {
			t.Fatalf("expected one conflict naming both UUIDs: %+v", r)
		}
		if len(sink.events) == 1 && !slices.Equal(r.MissingSinkEvents, []string{b.EventID.String()}) {
			t.Fatalf("lost UUID hidden by delivered seq: %+v", r)
		}
	}
}

func TestCompareChecksDestinationSequenceAndUser(t *testing.T) {
	e := testEvent("a", 1, `null`)
	other := e
	other.UserID = "b"
	r := compare([]event.Event{e}, nil, inSink(other))
	if !slices.Equal(r.MissingSinkEvents, []string{e.EventID.String()}) || !slices.Equal(r.MissingSequences, []string{"a:1"}) {
		t.Fatalf("UUID delivered with wrong user: %+v", r)
	}
	e.Payload = json.RawMessage(`{"poison":true}`)
	other = e
	other.Seq = 2
	r = compare([]event.Event{e}, []event.Event{other}, inSink())
	if !slices.Equal(r.PoisonNotInDLT, []string{e.EventID.String()}) {
		t.Fatalf("DLT UUID delivered with wrong seq: %+v", r)
	}
}

func TestCompareGapsAndWrongDestination(t *testing.T) {
	ordinary := testEvent("a", 1, `[]`)
	poison := testEvent("a", 3, `{"poison":true}`)
	r := compare([]event.Event{ordinary, poison}, []event.Event{ordinary}, inSink(poison))
	if !slices.Equal(r.MissingSequences, []string{"a:2"}) || !slices.Equal(r.NonPoisonInDLT, []string{ordinary.EventID.String()}) || !slices.Equal(r.PoisonNotInDLT, []string{poison.EventID.String()}) {
		t.Fatalf("unexpected gaps or routing: %+v", r)
	}
	r = compare([]event.Event{poison}, []event.Event{poison}, inSink(poison))
	if len(r.PoisonNotInDLT) != 1 {
		t.Fatalf("poison in both destinations: %+v", r)
	}
}
