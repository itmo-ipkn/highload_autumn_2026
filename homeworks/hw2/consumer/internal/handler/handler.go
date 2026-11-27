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
	"highload/hw2/consumer/internal/dlt"
	"highload/hw2/consumer/internal/event"
	"highload/hw2/consumer/internal/sink"
)

type Handler struct {
	podID          string
	delay          time.Duration
	commitInterval time.Duration
	sink           *sink.Sink
	logger         *slog.Logger
	dlt            *dlt.Publisher
}

func New(cfg config.Config, eventSink *sink.Sink, dltPublisher *dlt.Publisher, logger *slog.Logger) *Handler {
	return &Handler{
		podID: cfg.PodID, delay: cfg.ProcessingDelay, commitInterval: cfg.CommitInterval,
		sink: eventSink, dlt: dltPublisher, logger: logger,
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
			err := h.processWithRetry(session.Context(), message)
			if err != nil {
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

const processingAttempts = 3

func (h *Handler) processWithRetry(ctx context.Context, message *sarama.ConsumerMessage) error {
	var processingErr error
	for attempt := 1; attempt <= processingAttempts; attempt++ {
		processingErr = h.process(ctx, message)
		if processingErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt < processingAttempts {
			timer := time.NewTimer(time.Duration(attempt) * 100 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}
	}
	if err := h.dlt.Publish(message, processingErr, processingAttempts); err != nil {
		return fmt.Errorf("processing failed after %d attempts (%v), and DLT publish failed: %w", processingAttempts, processingErr, err)
	}
	h.logger.Warn("message moved to dead-letter topic",
		slog.String("topic", message.Topic),
		slog.Int("partition", int(message.Partition)),
		slog.Int64("offset", message.Offset),
		slog.Int("attempts", processingAttempts),
		slog.Any("error", processingErr),
	)
	return nil
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
	var payload struct {
		Poison bool `json:"poison"`
	}
	if err := json.Unmarshal(e.Payload, &payload); err != nil {
		return fmt.Errorf("decode event payload: %w", err)
	}
	if payload.Poison {
		return fmt.Errorf("poison event %s", e.EventID)
	}
	if err := h.sink.Store(ctx, sink.Record{
		Event: e, Consumer: h.podID, Topic: message.Topic,
		Partition: message.Partition, Offset: message.Offset,
	}); err != nil {
		return err
	}
	return nil
}
