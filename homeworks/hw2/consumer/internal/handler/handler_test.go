package handler

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"highload/hw2/consumer/internal/event"
	"highload/hw2/consumer/internal/metrics"
)

func TestProcessRejectsMalformedEvent(t *testing.T) {
	h := &Handler{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	message := &sarama.ConsumerMessage{Value: []byte("{")}

	err := h.process(context.Background(), message)
	if err == nil || !strings.Contains(err.Error(), "decode event") {
		t.Fatalf("process() error = %v, want event decoding error", err)
	}
}

func TestProcessRejectsPoisonEventBeforeSink(t *testing.T) {
	h := &Handler{
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics: metrics.New("test"),
	}
	e := event.Event{
		EventID: uuid.New(), UserID: "user-1", Type: event.TypePurchase, Seq: 1,
		ProducedAt: time.Now().UTC(), Payload: json.RawMessage(`{"poison":true}`),
	}
	value, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}

	err = h.process(context.Background(), &sarama.ConsumerMessage{Value: value})
	if err == nil || !strings.Contains(err.Error(), "poison event") {
		t.Fatalf("process() error = %v, want poison event error", err)
	}
}
