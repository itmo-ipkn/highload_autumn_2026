// Package handler implements the Kafka consumer-group callbacks.
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"

	"highload/hw2/consumer/internal/config"
	"highload/hw2/consumer/internal/event"
	"highload/hw2/consumer/internal/metrics"
	"highload/hw2/consumer/internal/sink"
)

type eventSink interface {
	Store(context.Context, sink.Record) (bool, error)
}

type deadLetterPublisher interface {
	Publish(context.Context, *sarama.ConsumerMessage, error, int) error
}

type Handler struct {
	podID          string
	delay          time.Duration
	commitInterval time.Duration
	sink           eventSink
	logger         *slog.Logger
	dlt            deadLetterPublisher
	metrics        *metrics.Metrics
}

func New(cfg config.Config, eventSink eventSink, dltPublisher deadLetterPublisher, logger *slog.Logger, m *metrics.Metrics) *Handler {
	return &Handler{
		podID: cfg.PodID, delay: cfg.ProcessingDelay, commitInterval: cfg.CommitInterval,
		sink: eventSink, dlt: dltPublisher, logger: logger, metrics: m,
	}
}

func (h *Handler) Setup(session sarama.ConsumerGroupSession) error {
	count := 0
	for _, partitions := range session.Claims() {
		count += len(partitions)
	}
	h.metrics.Setup(count)
	return nil
}
func (h *Handler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.metrics.Cleanup()
	session.Commit()
	return nil
}

func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ticker := time.NewTicker(h.commitInterval)
	defer ticker.Stop()
	first := true
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				session.Commit()
				return nil
			}
			if first {
				h.logger.Info("first claim message", slog.String("pod_id", h.podID), slog.String("topic", message.Topic),
					slog.Int("partition", int(message.Partition)), slog.Int64("offset", message.Offset))
				first = false
			}
			started := time.Now()
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
			h.metrics.Processed(message.Topic, message.Partition, message.Offset, false, time.Since(started))
		case <-ticker.C:
			session.Commit()
		case <-session.Context().Done():
			session.Commit()
			return nil
		}
	}
}

const processingAttempts = 3

var errInvalidEvent = errors.New("invalid event")

func (h *Handler) processWithRetry(ctx context.Context, message *sarama.ConsumerMessage) error {
	var processingErr error
	for attempt := 1; attempt <= processingAttempts; attempt++ {
		if attempt > 1 {
			h.metrics.Retried()
		}
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if !errors.Is(processingErr, errInvalidEvent) {
		return processingErr
	}
	if err := h.dlt.Publish(ctx, message, processingErr, processingAttempts); err != nil {
		return fmt.Errorf("processing failed after %d attempts (%v), and DLT publish failed: %w", processingAttempts, processingErr, err)
	}
	h.metrics.DLT(message.Topic)
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
		return fmt.Errorf("%w: decode event: %w", errInvalidEvent, err)
	}
	if err := e.Validate(); err != nil {
		return fmt.Errorf("%w: validate event: %w", errInvalidEvent, err)
	}
	if event.IsPoison(e.Payload) {
		return fmt.Errorf("%w: poison event %s", errInvalidEvent, e.EventID)
	}
	inserted, err := h.sink.Store(ctx, sink.Record{
		Event: e, Consumer: h.podID, Topic: message.Topic,
		Partition: message.Partition, Offset: message.Offset,
	})
	if err != nil {
		return err
	}
	if !inserted {
		h.metrics.Duplicate(message.Topic)
	}
	return nil
}
