package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"kafka-events/internal/config"
	"kafka-events/internal/consumer"
	"kafka-events/internal/model"
	"kafka-events/internal/storage"
)

func main() {
	cfg, err := config.LoadConsumerConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbCtx, dbCancel := context.WithTimeout(ctx, 25*time.Second)
	defer dbCancel()

	db, err := storage.New(dbCtx, cfg.PgURL)
	if err != nil {
		log.Fatalf("failed to connect to db: %v", err)
	}
	defer db.Close()

	state := consumer.NewConsumerState()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ConsumerID),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelInfo, func() string { return cfg.ConsumerID })),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, partitionsByTopic map[string][]int32) {
			log.Printf("REBALANCE assigned=%v", partitionsByTopic)
			state.MarkAssigned(partitionsByTopic)
		}),
		kgo.OnPartitionsRevoked(func(callbackCtx context.Context, client *kgo.Client, partitionsByTopic map[string][]int32) {
			log.Printf("REBALANCE revoked=%v, committing last processed offsets", partitionsByTopic)
			records := state.RecordsForCommit(partitionsByTopic)
			if len(records) > 0 {
				commitCtx, cancel := context.WithTimeout(context.WithoutCancel(callbackCtx), 10*time.Second)
				if err := client.CommitRecords(commitCtx, records...); err != nil {
					log.Printf("REBALANCE revoke commit failed: %v", err)
				}
				cancel()
			}
			state.DeletePartitions(partitionsByTopic)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, partitionsByTopic map[string][]int32) {
			log.Printf("REBALANCE lost=%v, ownership is gone, no offset commit is attempted", partitionsByTopic)
			state.DeletePartitions(partitionsByTopic)
		}),
	)
	if err != nil {
		log.Fatalf("failed to create kafka consumer client: %v", err)
	}
	defer client.CloseAllowingRebalance()

	log.Printf("consumer started consumerID=%s groupID=%s topic=%s dlttopic=%s processing_delay=%s processing_jitter=%s poll_records=%d, max_retries=%d",
		cfg.ConsumerID, cfg.GroupID, cfg.Topic, cfg.DLTTopic, cfg.ProcessingDelay, cfg.ProcessingJitter, cfg.PollRecords, cfg.MaxRetries)

	for ctx.Err() == nil {
		fetches := client.PollRecords(ctx, cfg.PollRecords)
		if fetches.IsClientClosed() {
			client.AllowRebalance()
			break
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				log.Printf("fetch error topic=%s partition=%d: %v", topic, partition, err)
			}
		})

		var processErr error
		var toCommit []*kgo.Record
		for _, record := range fetches.Records() {
			if ctx.Err() != nil {
				break
			}

			key := consumer.PartitionKey(record.Topic, record.Partition)
			if state.ShouldLogResume(key) {
				log.Printf("RESUME consumer=%s topic=%s partition=%d offset=%d", cfg.ConsumerID, record.Topic, record.Partition, record.Offset)
			}

			if err := processRecord(ctx, db, client, record, &cfg); err != nil {
				processErr = err
				break
			}

			toCommit = append(toCommit, record)
		}

		if len(toCommit) > 0 {
			commitBaseCtx := ctx
			if commitBaseCtx.Err() != nil {
				commitBaseCtx = context.WithoutCancel(ctx)
			}

			commitCtx, cancel := context.WithTimeout(commitBaseCtx, 5*time.Second)
			err := client.CommitRecords(commitCtx, toCommit...)
			cancel()

			if err != nil {
				if ctx.Err() == nil {
					processErr = fmt.Errorf("commit batch with %d records: %w", len(toCommit), err)
				}
			} else {
				for _, rec := range toCommit {
					key := consumer.PartitionKey(rec.Topic, rec.Partition)
					if !state.SetLastCommitted(key, rec) {
						log.Printf("partition state missing after commit key=%s offset=%d", key, rec.Offset)
					}
				}
			}
		}

		client.AllowRebalance()
		if processErr != nil {
			if ctx.Err() != nil {
				break
			}

			log.Printf("consumer stopped after processing/commit error: %v", processErr)
			break
		}
	}

	log.Printf("consumer stopped id=%s", cfg.ConsumerID)
}

func processRecord(ctx context.Context, db *storage.Store, client *kgo.Client, record *kgo.Record, cfg *config.ConsumerConfig) error {
	var event model.Event
	decodeErr := json.Unmarshal(record.Value, &event)

	var processingErr error
	switch {
	case decodeErr != nil:
		processingErr = fmt.Errorf("invalid event JSON: %w", decodeErr)
	case model.IsPoison(event):
		processingErr = errors.New("poison event: payload.poison=true")
	}

	if processingErr != nil {
		for attempt := 1; attempt <= cfg.MaxRetries; attempt++ {
			if err := wait(ctx, retryDelay(cfg.ProcessingDelay, cfg.ProcessingJitter)); err != nil {
				return err
			}

			log.Printf("processing failed attempt=%d/%d event_id=%s user_id=%s seq=%d reason=%v",
				attempt, cfg.MaxRetries, event.EventID, event.UserID, event.Seq, processingErr,
			)
		}

		return publishDLT(ctx, client, record, cfg, processingErr)
	}

	if err := wait(ctx, retryDelay(cfg.ProcessingDelay, cfg.ProcessingJitter)); err != nil {
		return err
	}

	backoff := 250 * time.Millisecond
	for {
		inserted, err := db.SaveSinkEvent(ctx, event, cfg.ConsumerID, record.Topic, record.Partition, record.Offset)
		if err == nil {
			if !inserted {
				log.Printf("deduplicated event_id=%s user_id=%s seq=%d", event.EventID, event.UserID, event.Seq)
			}
			return nil
		}

		log.Printf("sink write failed, retrying event_id=%s: %v", event.EventID, err)
		if err := wait(ctx, backoff); err != nil {
			return err
		}

		backoff = min(backoff*2, 5*time.Second)
	}
}

func publishDLT(ctx context.Context, client *kgo.Client, original *kgo.Record, cfg *config.ConsumerConfig, reason error) error {
	backoff := 250 * time.Millisecond
	for {
		dlt := &kgo.Record{
			Topic: cfg.DLTTopic,
			Key:   original.Key,
			Value: original.Value,
			Headers: []kgo.RecordHeader{
				{Key: "dlt-error", Value: []byte(reason.Error())},
				{Key: "dlt-original-topic", Value: []byte(original.Topic)},
				{Key: "dlt-original-partition", Value: []byte(strconv.Itoa(int(original.Partition)))},
				{Key: "dlt-original-offset", Value: []byte(strconv.FormatInt(original.Offset, 10))},
			},
		}

		if err := client.ProduceSync(ctx, dlt).FirstErr(); err == nil {
			log.Printf("sent to DLT original topic=%s partition=%d offset=%d reason=%q", original.Topic, original.Partition, original.Offset, reason)
			return nil
		} else {
			log.Printf("DLT publish failed, source offset remains uncommitted original topic=%s partition=%d offset=%d: %v",
				original.Topic, original.Partition, original.Offset, err)
		}

		if err := wait(ctx, backoff); err != nil {
			return err
		}

		backoff = min(backoff*2, 5*time.Second)
	}
}

func retryDelay(base time.Duration, jitter time.Duration) time.Duration {
	return base + time.Duration(rand.Int64N(int64(jitter)+1))
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
