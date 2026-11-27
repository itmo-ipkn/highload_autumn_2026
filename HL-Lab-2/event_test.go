package lab2

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEventJSON(t *testing.T) {
	event := Event{
		EventID:    uuid.New(),
		UserID:     uuid.New(),
		Type:       PageView,
		Seq:        1,
		ProducedAt: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC),
		Payload:    json.RawMessage(`{"path":"/home"}`),
		Poison:     true,
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Event
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.EventID != event.EventID || decoded.UserID != event.UserID || decoded.Type != event.Type || decoded.Seq != 1 || !decoded.ProducedAt.Equal(event.ProducedAt) || string(decoded.Payload) != string(event.Payload) || !decoded.Poison {
		t.Fatalf("event changed after JSON round trip: %+v", decoded)
	}
}
