package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Steplet/HL-lab-2"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
)

func deadLetterRecord(source *kgo.Record, reason string) *kgo.Record {
	return &kgo.Record{
		Topic: "events.dlt",
		Key:   source.Key,
		Value: source.Value,
		Headers: []kgo.RecordHeader{
			{Key: "error_reason", Value: []byte(reason)},
			{Key: "source_topic", Value: []byte(source.Topic)},
			{Key: "source_partition", Value: []byte(strconv.Itoa(int(source.Partition)))},
			{Key: "source_offset", Value: []byte(strconv.FormatInt(source.Offset, 10))},
		},
	}
}

func writeToSink(ctx context.Context, db *pgx.Conn, event lab2.Event, record *kgo.Record, podID string) (bool, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_events (event_id, user_id, seq, consumer_pod, source_topic, source_partition, source_offset)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (event_id) DO NOTHING`,
		event.EventID, event.UserID, event.Seq, podID, record.Topic, record.Partition, record.Offset)
	if err != nil {
		return false, err
	}
	duplicate := tag.RowsAffected() == 0
	if duplicate {
		if _, err := tx.Exec(ctx, `UPDATE duplicate_attempts SET count = count + 1 WHERE id = 1`); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return duplicate, nil
}

func processRecord(ctx context.Context, client *kgo.Client, db *pgx.Conn, record *kgo.Record, podID string, delay time.Duration) error {
	var event lab2.Event
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return fmt.Errorf("decode event at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if !event.Poison {
			duplicate, err := writeToSink(ctx, db, event, record, podID)
			if err != nil {
				return err
			}
			log.Printf("sink pod=%s topic=%s partition=%d offset=%d user_id=%s seq=%d duplicate=%t", podID, record.Topic, record.Partition, record.Offset, event.UserID, event.Seq, duplicate)
			return nil
		}
		log.Printf("processing failed pod=%s event_id=%s attempt=%d/3: poison event", podID, event.EventID, attempt)
	}

	if err := client.ProduceSync(ctx, deadLetterRecord(record, "poison event")).FirstErr(); err != nil {
		return fmt.Errorf("send event %s to DLT: %w", event.EventID, err)
	}
	log.Printf("DLT pod=%s topic=%s partition=%d offset=%d event_id=%s", podID, record.Topic, record.Partition, record.Offset, event.EventID)
	return nil
}

func run() error {
	podID := os.Getenv("POD_ID")
	brokers := os.Getenv("KAFKA_BROKERS")
	dsn := os.Getenv("PG_DSN")
	if podID == "" || brokers == "" || dsn == "" {
		return errors.New("POD_ID, KAFKA_BROKERS and PG_DSN are required")
	}
	delayMS, err := strconv.Atoi(os.Getenv("PROCESSING_DELAY_MS"))
	if err != nil || delayMS < 0 {
		return errors.New("PROCESSING_DELAY_MS must be a non-negative integer")
	}
	delay := time.Duration(delayMS) * time.Millisecond
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close(context.Background())

	var mu sync.Mutex
	lastProcessed := make(map[int32]*kgo.Record)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ConsumerGroup("events-processor"),
		kgo.ConsumeTopics("events"),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxPartitionBytes(1024),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			log.Printf("assigned pod=%s partitions=%v", podID, partitions)
		}),
		kgo.OnPartitionsRevoked(func(rebalanceCtx context.Context, cl *kgo.Client, partitions map[string][]int32) {
			log.Printf("revoked pod=%s partitions=%v", podID, partitions)
			mu.Lock()
			var records []*kgo.Record
			for _, partition := range partitions["events"] {
				if record := lastProcessed[partition]; record != nil {
					records = append(records, record)
					delete(lastProcessed, partition)
				}
			}
			mu.Unlock()
			if len(records) > 0 {
				commitCtx, cancel := context.WithTimeout(rebalanceCtx, 10*time.Second)
				defer cancel()
				if err := cl.CommitRecords(commitCtx, records...); err != nil {
					log.Printf("rebalance commit failed pod=%s: %v", podID, err)
				}
			}
		}),
	)
	if err != nil {
		return err
	}
	defer client.Close()
	log.Printf("consumer pod=%s delay=%s reset=earliest", podID, delay)

	for {
		fetches := client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			client.AllowRebalance()
			return nil
		}
		for _, fetchErr := range fetches.Errors() {
			log.Printf("fetch error pod=%s: %v", podID, fetchErr)
		}
		for _, record := range fetches.Records() {
			if err := processRecord(ctx, client, db, record, podID, delay); err != nil {
				client.AllowRebalance()
				return fmt.Errorf("process %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
			}
			mu.Lock()
			lastProcessed[record.Partition] = record
			mu.Unlock()
			if err := client.CommitRecords(ctx, record); err != nil {
				client.AllowRebalance()
				return fmt.Errorf("commit %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
			}
		}
		client.AllowRebalance()
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
