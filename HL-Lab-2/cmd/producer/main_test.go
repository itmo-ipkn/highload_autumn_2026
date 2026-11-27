package main

import (
	"testing"

	"github.com/Steplet/HL-lab-2"
	"github.com/google/uuid"
)

func TestMakeEvent(t *testing.T) {
	userID := uuid.New()
	for _, tc := range []struct {
		seq    uint64
		typeID lab2.EventType
		poison bool
	}{
		{1, lab2.PageView, false},
		{2, lab2.AddToCart, false},
		{3, lab2.Purchase, false},
		{50, lab2.AddToCart, true},
		{51, lab2.Purchase, false},
	} {
		event := makeEvent(userID, tc.seq)
		if event.EventID == uuid.Nil || event.UserID != userID || event.Seq != tc.seq || event.Type != tc.typeID || event.Poison != tc.poison || event.ProducedAt.IsZero() || len(event.Payload) == 0 {
			t.Fatalf("unexpected event for seq %d: %+v", tc.seq, event)
		}
	}
}
