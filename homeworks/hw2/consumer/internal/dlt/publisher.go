// Package dlt publishes messages that could not be processed to a dead-letter topic.
package dlt

import (
	"fmt"
	"strconv"
	"time"

	"github.com/IBM/sarama"
)

type messageProducer interface {
	SendMessage(*sarama.ProducerMessage) (partition int32, offset int64, err error)
	Close() error
}

type Publisher struct {
	producer messageProducer
	topic    string
	podID    string
}

func Open(brokers []string, version sarama.KafkaVersion, topic, podID string) (*Publisher, error) {
	cfg := sarama.NewConfig()
	cfg.Version = version
	// SyncProducer has no request context, so keep its retry and network budget bounded.
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Idempotent = true
	cfg.Producer.Retry.Max = 3
	cfg.Producer.Retry.Backoff = 100 * time.Millisecond
	cfg.Producer.Timeout = 5 * time.Second
	cfg.Producer.Return.Successes = true
	cfg.Net.DialTimeout = 3 * time.Second
	cfg.Net.ReadTimeout = 5 * time.Second
	cfg.Net.WriteTimeout = 5 * time.Second
	cfg.Net.MaxOpenRequests = 1
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate DLT producer config: %w", err)
	}
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("create DLT producer: %w", err)
	}
	return &Publisher{producer: producer, topic: topic, podID: podID}, nil
}

func (p *Publisher) Close() error { return p.producer.Close() }

func (p *Publisher) Publish(original *sarama.ConsumerMessage, cause error, attempts int) error {
	message := &sarama.ProducerMessage{
		Topic: p.topic,
		Value: sarama.ByteEncoder(original.Value),
		Headers: []sarama.RecordHeader{
			{Key: []byte("x-error"), Value: []byte(cause.Error())},
			{Key: []byte("x-original-topic"), Value: []byte(original.Topic)},
			{Key: []byte("x-original-partition"), Value: []byte(strconv.FormatInt(int64(original.Partition), 10))},
			{Key: []byte("x-original-offset"), Value: []byte(strconv.FormatInt(original.Offset, 10))},
			{Key: []byte("x-attempts"), Value: []byte(strconv.Itoa(attempts))},
			{Key: []byte("x-consumer-pod"), Value: []byte(p.podID)},
		},
	}
	if original.Key != nil {
		message.Key = sarama.ByteEncoder(original.Key)
	}
	if _, _, err := p.producer.SendMessage(message); err != nil {
		return fmt.Errorf("publish message to %s: %w", p.topic, err)
	}
	return nil
}
