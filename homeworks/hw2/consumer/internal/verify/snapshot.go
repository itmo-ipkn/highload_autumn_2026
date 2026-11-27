package verify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/IBM/sarama"

	"highload/hw2/consumer/internal/event"
)

func readTopicSnapshot(ctx context.Context, client sarama.Client, topic string) ([]event.Event, error) {
	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, fmt.Errorf("list partitions of %s: %w", topic, err)
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("create consumer for %s: %w", topic, err)
	}
	defer consumer.Close()

	var events []event.Event
	for _, partition := range partitions {
		start, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
		if err != nil {
			return nil, fmt.Errorf("get start of %s[%d]: %w", topic, partition, err)
		}
		end, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return nil, fmt.Errorf("get end of %s[%d]: %w", topic, partition, err)
		}
		part, err := readPartition(ctx, consumer, topic, partition, start, end)
		if err != nil {
			return nil, fmt.Errorf("read %s[%d] offsets [%d,%d): %w", topic, partition, start, end, err)
		}
		events = append(events, part...)
	}
	return events, nil
}

func readPartition(ctx context.Context, consumer sarama.Consumer, topic string, partition int32, start, end int64) (events []event.Event, err error) {
	if start >= end {
		return nil, nil
	}
	pc, err := consumer.ConsumePartition(topic, partition, start)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, pc.Close()) }()
	messages, errs := pc.Messages(), pc.Errors()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case consumeErr, ok := <-errs:
			if !ok {
				errs = nil
			} else if consumeErr != nil {
				return nil, consumeErr
			}
		case msg, ok := <-messages:
			if !ok {
				return nil, errors.New("partition closed before end of snapshot")
			}
			if msg.Offset >= end {
				return events, nil
			}
			var e event.Event
			if err := json.Unmarshal(msg.Value, &e); err != nil {
				return nil, fmt.Errorf("decode offset %d: %w", msg.Offset, err)
			}
			if err := e.Validate(); err != nil {
				return nil, fmt.Errorf("validate offset %d: %w", msg.Offset, err)
			}
			events = append(events, e)
			if msg.Offset == end-1 {
				return events, nil
			}
		}
	}
}
