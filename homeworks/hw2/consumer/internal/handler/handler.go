// Package handler implements the Kafka consumer-group callbacks.
package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"

	"highload/hw2/consumer/internal/config"
	"highload/hw2/consumer/internal/event"
	"highload/hw2/consumer/internal/sink"
)

type Handler struct {
	podID          string
	delay          time.Duration
	commitInterval time.Duration
	sink           *sink.Sink
	logger         *slog.Logger
}

func New(cfg config.Config, eventSink *sink.Sink, logger *slog.Logger) *Handler {
	return &Handler{
		podID: cfg.PodID, delay: cfg.ProcessingDelay, commitInterval: cfg.CommitInterval,
		sink: eventSink, logger: logger,
	}
}

func (*Handler) Setup(sarama.ConsumerGroupSession) error { return nil }
func (*Handler) Cleanup(session sarama.ConsumerGroupSession) error {
	session.Commit()
	return nil
}

func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ticker := time.NewTicker(h.commitInterval)
	defer ticker.Stop()
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				session.Commit()
				return nil
			}
			if err := h.process(session.Context(), message); err != nil {
				h.logger.Error("message processing failed; offset will not be committed",
					slog.String("topic", message.Topic),
					slog.Int("partition", int(message.Partition)),
					slog.Int64("offset", message.Offset),
					slog.Any("error", err),
				)
				return err
			}
			session.MarkMessage(message, "")
		case <-ticker.C:
			session.Commit()
		case <-session.Context().Done():
			session.Commit()
			return nil
		}
	}
}

func (h *Handler) process(ctx context.Context, message *sarama.ConsumerMessage) error {
	if h.delay > 0 {
		timer := time.NewTimer(h.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var e event.Event
	if err := json.Unmarshal(message.Value, &e); err != nil {
		return fmt.Errorf("decode event: %w", err)
	}
	if err := e.Validate(); err != nil {
		return fmt.Errorf("validate event: %w", err)
	}
	if err := h.sink.Store(ctx, sink.Record{
		Event: e, Consumer: h.podID, Topic: message.Topic,
		Partition: message.Partition, Offset: message.Offset,
	}); err != nil {
		return err
	}
	return nil
}
