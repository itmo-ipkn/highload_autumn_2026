// Package verify checks the consumer sink against the Kafka dead-letter topic.
package verify

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sort"

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"highload/hw2/consumer/internal/event"
)

type Config struct {
	Brokers       []string
	KafkaVersion  sarama.KafkaVersion
	EventsTopic   string
	DLTTopic      string
	ConsumerGroup string
	PostgresURL   string
}

type Result struct {
	DLTEvents           int
	UniqueDLTEvents     int
	DuplicateDLTEvents  int
	PoisonEvents        int
	SinkEvents          int64
	DuplicateDeliveries int64
	MissingSequences    []string
	PoisonNotInDLT      []string
	NonPoisonInDLT      []string
	OrderViolations     int64
	ConsumerLag         int64
}

// Run reads the DLT up to each partition's high watermark and compares it with the sink.
func Run(ctx context.Context, cfg Config, logger *log.Logger) (Result, error) {
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

	sourceEvents, err := readDLT(ctx, kafkaClient, cfg.EventsTopic)
	if err != nil {
		return Result{}, fmt.Errorf("read source topic: %w", err)
	}
	dltEvents, err := readDLT(ctx, kafkaClient, cfg.DLTTopic)
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

	result, err := compare(ctx, db, sourceEvents, dltEvents)
	if err != nil {
		return Result{}, err
	}
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

func readDLT(ctx context.Context, client sarama.Client, topic string) ([]event.Event, error) {
	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, fmt.Errorf("list DLT partitions: %w", err)
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("create DLT consumer: %w", err)
	}
	defer consumer.Close()

	var events []event.Event
	for _, partition := range partitions {
		end, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return nil, fmt.Errorf("get DLT high watermark for partition %d: %w", partition, err)
		}
		start, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
		if err != nil {
			return nil, fmt.Errorf("get DLT start offset for partition %d: %w", partition, err)
		}
		if start >= end {
			continue
		}
		pc, err := consumer.ConsumePartition(topic, partition, start)
		if err != nil {
			return nil, fmt.Errorf("consume DLT partition %d: %w", partition, err)
		}
		readPartition := func() error {
			for {
				select {
				case message, ok := <-pc.Messages():
					if !ok {
						return fmt.Errorf("DLT partition %d closed before offset %d", partition, end)
					}
					if message.Offset >= end {
						return nil
					}
					var e event.Event
					if err := json.Unmarshal(message.Value, &e); err != nil {
						return fmt.Errorf("decode DLT message %s[%d] offset %d: %w", topic, partition, message.Offset, err)
					}
					if err := e.Validate(); err != nil {
						return fmt.Errorf("validate DLT message %s[%d] offset %d: %w", topic, partition, message.Offset, err)
					}
					events = append(events, e)
				case consumerErr, ok := <-pc.Errors():
					if ok && consumerErr != nil {
						return fmt.Errorf("consume topic %s partition %d: %w", topic, partition, consumerErr.Err)
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		readErr := readPartition()
		if err := pc.Close(); err != nil && readErr == nil {
			readErr = err
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	return events, nil
}

type sequenceSet map[string]map[uint64]struct{}

func (s sequenceSet) add(user string, seq uint64) {
	if s[user] == nil {
		s[user] = make(map[uint64]struct{})
	}
	s[user][seq] = struct{}{}
}

func compare(ctx context.Context, db *sql.DB, sourceEvents, dltEvents []event.Event) (Result, error) {
	result := Result{DLTEvents: len(dltEvents)}
	sequences := make(sequenceSet)
	expectedMaxSeq := make(map[string]uint64)
	dltIDs := make(map[uuid.UUID]struct{}, len(dltEvents))
	poisonIDs := make(map[uuid.UUID]struct{})
	expectedPoisonIDs := make(map[uuid.UUID]struct{})
	for _, e := range sourceEvents {
		if e.Seq > expectedMaxSeq[e.UserID] {
			expectedMaxSeq[e.UserID] = e.Seq
		}
		var payload struct {
			Poison bool `json:"poison"`
		}
		if err := json.Unmarshal(e.Payload, &payload); err != nil {
			return Result{}, fmt.Errorf("decode source payload for event %s: %w", e.EventID, err)
		}
		if payload.Poison {
			expectedPoisonIDs[e.EventID] = struct{}{}
		}
	}
	for _, e := range dltEvents {
		if _, exists := dltIDs[e.EventID]; exists {
			result.DuplicateDLTEvents++
			continue
		}
		dltIDs[e.EventID] = struct{}{}
		result.UniqueDLTEvents++
		sequences.add(e.UserID, e.Seq)
		var payload struct {
			Poison bool `json:"poison"`
		}
		if err := json.Unmarshal(e.Payload, &payload); err != nil {
			return Result{}, fmt.Errorf("decode payload for DLT event %s: %w", e.EventID, err)
		}
		if payload.Poison {
			result.PoisonEvents++
			poisonIDs[e.EventID] = struct{}{}
		} else {
			result.NonPoisonInDLT = append(result.NonPoisonInDLT, e.EventID.String())
		}
	}

	rows, err := db.QueryContext(ctx, `SELECT event_id, user_id, seq FROM processed_events`)
	if err != nil {
		return Result{}, fmt.Errorf("query processed events: %w", err)
	}
	sinkIDs := make(map[uuid.UUID]struct{})
	for rows.Next() {
		var id uuid.UUID
		var user string
		var seq int64
		if err := rows.Scan(&id, &user, &seq); err != nil {
			rows.Close()
			return Result{}, fmt.Errorf("scan processed event: %w", err)
		}
		sinkIDs[id] = struct{}{}
		sequences.add(user, uint64(seq))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return Result{}, fmt.Errorf("iterate processed events: %w", err)
	}
	if err := rows.Close(); err != nil {
		return Result{}, fmt.Errorf("close processed events rows: %w", err)
	}
	result.SinkEvents = int64(len(sinkIDs))

	for id := range expectedPoisonIDs {
		if _, exists := poisonIDs[id]; !exists {
			result.PoisonNotInDLT = append(result.PoisonNotInDLT, id.String())
		}
	}
	for id := range poisonIDs {
		if _, exists := sinkIDs[id]; exists {
			result.PoisonNotInDLT = append(result.PoisonNotInDLT, id.String()+" (also in sink)")
		}
	}
	var duplicates int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM duplicate_deliveries`).Scan(&duplicates); err != nil {
		return Result{}, fmt.Errorf("count duplicate deliveries: %w", err)
	}
	result.DuplicateDeliveries = duplicates
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM (
			SELECT seq, max(seq) OVER (
				PARTITION BY user_id ORDER BY write_id
				ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
			) AS previous_max
			FROM processed_events
		) ordered
		WHERE previous_max IS NOT NULL AND seq < previous_max`).Scan(&result.OrderViolations); err != nil {
		return Result{}, fmt.Errorf("check processing order: %w", err)
	}

	for user, max := range expectedMaxSeq {
		values := sequences[user]
		for seq := uint64(1); seq <= max; seq++ {
			if _, exists := values[seq]; !exists {
				result.MissingSequences = append(result.MissingSequences, fmt.Sprintf("%s:%d", user, seq))
			}
		}
	}
	sort.Strings(result.MissingSequences)
	sort.Strings(result.PoisonNotInDLT)
	sort.Strings(result.NonPoisonInDLT)
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
			committed = block.Offset
		}
		if end > committed {
			lag += end - committed
		}
	}
	return lag, nil
}

func (r Result) Passed() bool {
	return len(r.MissingSequences) == 0 && len(r.PoisonNotInDLT) == 0 &&
		len(r.NonPoisonInDLT) == 0 && r.OrderViolations == 0
}
