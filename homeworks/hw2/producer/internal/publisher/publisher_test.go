package publisher

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/google/uuid"

	"highload/hw2/producer/internal/event"
)

func TestPublishReturnsAsyncDeliveryError(t *testing.T) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	mock := mocks.NewAsyncProducer(t, cfg)
	wantErr := errors.New("terminal delivery error")
	mock.ExpectInputAndFail(wantErr)

	p, err := NewBuilder(Options{Topic: "events", MaxInFlight: 1}).
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))).
		WithProducer(mock).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	t.Cleanup(func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), time.Second)
		defer cancelClose()
		if err := p.Close(closeCtx); err != nil {
			t.Errorf("close publisher: %v", err)
		}
	})

	e := event.Event{
		EventID:    uuid.New(),
		UserID:     "user-1",
		Type:       event.TypePageView,
		Seq:        1,
		ProducedAt: time.Now().UTC(),
		Payload:    []byte(`{"url":"/"}`),
	}
	if err := p.Publish(ctx, e); !errors.Is(err, wantErr) {
		t.Fatalf("Publish() error = %v, want %v", err, wantErr)
	}

	stats := p.Stats()
	if stats.Enqueued != 1 || stats.Acked != 0 || stats.Failed != 1 || stats.InFlight != 0 {
		t.Fatalf("stats after failed delivery = %+v", stats)
	}
}
