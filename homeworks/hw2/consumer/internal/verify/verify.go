// Package verify checks the consumer sink against the Kafka dead-letter topic.
package verify

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/IBM/sarama"

	"highload/hw2/consumer/internal/config"
)

type Result struct {
	DLTEvents           int
	UniqueDLTEvents     int
	DuplicateDLTEvents  int
	PoisonEvents        int
	SinkEvents          int64
	DuplicateDeliveries int64
	MissingSequences    []string
	SequenceConflicts   []string
	MissingSinkEvents   []string
	PoisonNotInDLT      []string
	NonPoisonInDLT      []string
	OrderViolations     int64
	ConsumerLag         int64
}

// Run compares snapshots of the source and dead-letter topics with the sink.
func Run(ctx context.Context, cfg config.VerifyConfig, logger *log.Logger) (Result, error) {
	clientCfg := sarama.NewConfig()
	clientCfg.Version = cfg.KafkaVersion
	clientCfg.Consumer.Return.Errors = true
	admin, err := sarama.NewClusterAdmin(cfg.Brokers, clientCfg)
	if err != nil {
		return Result{}, fmt.Errorf("create Kafka admin: %w", err)
	}
	defer admin.Close()

	kafkaClient, err := sarama.NewClient(cfg.Brokers, clientCfg)
	if err != nil {
		return Result{}, fmt.Errorf("create Kafka client: %w", err)
	}
	defer kafkaClient.Close()

	sourceEvents, err := readTopicSnapshot(ctx, kafkaClient, cfg.EventsTopic)
	if err != nil {
		return Result{}, fmt.Errorf("read source topic: %w", err)
	}
	dltEvents, err := readTopicSnapshot(ctx, kafkaClient, cfg.DLTTopic)
	if err != nil {
		return Result{}, err
	}

	db, err := sql.Open("pgx", cfg.PostgresURL)
	if err != nil {
		return Result{}, fmt.Errorf("open Postgres: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return Result{}, fmt.Errorf("connect to Postgres: %w", err)
	}

	sink, err := readSink(ctx, db)
	if err != nil {
		return Result{}, err
	}
	result := compare(sourceEvents, dltEvents, sink)
	result.ConsumerLag, err = consumerLag(admin, kafkaClient, cfg.ConsumerGroup, cfg.EventsTopic)
	if err != nil {
		return Result{}, err
	}
	if logger != nil {
		logger.Printf("DLT: %d messages, %d unique events (%d duplicates), %d poison events",
			result.DLTEvents, result.UniqueDLTEvents, result.DuplicateDLTEvents, result.PoisonEvents)
		logger.Printf("sink: %d unique events, %d duplicate deliveries", result.SinkEvents, result.DuplicateDeliveries)
		if result.ConsumerLag > 0 {
			logger.Printf("WARNING: consumer group %q has %d events of lag; run verify after it catches up", cfg.ConsumerGroup, result.ConsumerLag)
		} else {
			logger.Printf("consumer group %q lag: 0", cfg.ConsumerGroup)
		}
	}
	return result, nil
}

func consumerLag(admin sarama.ClusterAdmin, client sarama.Client, group, topic string) (int64, error) {
	partitions, err := client.Partitions(topic)
	if err != nil {
		return 0, fmt.Errorf("list event topic partitions: %w", err)
	}
	response, err := admin.ListConsumerGroupOffsets(group, map[string][]int32{topic: partitions})
	if err != nil {
		return 0, fmt.Errorf("fetch consumer group offsets: %w", err)
	}
	if response.GroupError() != sarama.ErrNoError {
		return 0, fmt.Errorf("fetch consumer group offsets: %w", response.GroupError())
	}
	var lag int64
	for _, partition := range partitions {
		end, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return 0, fmt.Errorf("get event high watermark for partition %d: %w", partition, err)
		}
		block := response.GetBlock(topic, partition)
		committed := int64(0)
		if block != nil {
			if block.Err != sarama.ErrNoError {
				return 0, fmt.Errorf("get committed offset for %s[%d]: %w", topic, partition, block.Err)
			}
			committed = max(0, block.Offset)
		}
		if end > committed {
			lag += end - committed
		}
	}
	return lag, nil
}
