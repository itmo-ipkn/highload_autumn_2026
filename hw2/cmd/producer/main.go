package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafka-events/internal/config"
	"kafka-events/internal/model"
)

func main() {
	cfg, err := config.LoadProducerConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ProducerID),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelInfo, func() string { return cfg.ProducerID })),
		kgo.RecordRetries(cfg.RetriesCount),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProduceRequestTimeout(cfg.RequestTimeout),
	}

	if cfg.KeyMode == model.KeyModeNone {
		opts = append(opts, kgo.RecordPartitioner(kgo.RoundRobinPartitioner()))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("failed to create kafka producer client: %v", err)
	}
	defer client.Close()

	runID := uuid.NewString()
	users := make([]string, cfg.UsersCount)
	for i := range users {
		users[i] = fmt.Sprintf("%s-%s-user-%d", cfg.ProducerID, runID, i+1)
	}

	seqByUser := make([]int64, cfg.UsersCount)
	interval := max(time.Second/time.Duration(cfg.EventsPerSec), time.Millisecond)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("producer started id=%s run_id=%s users=%d events_per_sec=%d key_mode=%s",
		cfg.ProducerID, runID, cfg.UsersCount, cfg.EventsPerSec, cfg.KeyMode)

	producedCount := 0
	userIndex := 0

loop:
	for {
		select {
		case <-ctx.Done():
			log.Printf("shutdown producer_id=%s err=%v", cfg.ProducerID, ctx.Err())
			break loop
		case <-ticker.C:
			userID := users[userIndex]
			nextSeq := seqByUser[userIndex] + 1

			event := newEvent(&cfg, userID, nextSeq)
			record, err := newRecord(event, cfg.KeyMode)
			if err != nil {
				log.Printf("failed to create record for user_id=%s seq=%d: %v", userID, nextSeq, err)
				continue
			}

			if err := publishEvent(ctx, client, record, cfg.DeliveryTimeout); err != nil {
				if ctx.Err() != nil {
					log.Printf("event delivery stopped due to shutdown producer_id=%s event_id=%s user_id=%s seq=%d err=%v",
						cfg.ProducerID, event.EventID, event.UserID, event.Seq, err)
					break loop
				}
				log.Printf("failed to deliver event event_id=%s user_id=%s seq=%d err=%v",
					event.EventID, event.UserID, event.Seq, err)
				continue
			}

			seqByUser[userIndex] = nextSeq
			producedCount++
			userIndex = (userIndex + 1) % len(users)
			if producedCount%100 == 0 {
				log.Printf("producer_id=%s produced_count=%d", cfg.ProducerID, producedCount)
			}
		}
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Flush(flushCtx); err != nil {
		log.Printf("kafka producer client flush failed: %v", err)
	} else {
		log.Printf("kafka producer successfully stopped with produced_count=%d", producedCount)
	}
}

func newEvent(cfg *config.ProducerConfig, userID string, seq int64) model.Event {
	payload, _ := json.Marshal(model.Payload{
		Poison:     rand.Float64() < cfg.PoisonRate,
		ProducerID: cfg.ProducerID,
		Message:    "load event",
	})

	return model.Event{
		EventID:    uuid.NewString(),
		UserID:     userID,
		Type:       model.EventTypes[rand.IntN(len(model.EventTypes))],
		Seq:        seq,
		ProducedAt: time.Now().UTC(),
		Payload:    payload,
	}
}

func newRecord(event model.Event, keyMode string) (*kgo.Record, error) {
	value, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	var key []byte
	if keyMode == model.KeyModeUserId {
		key = []byte(event.UserID)
	}

	return &kgo.Record{Topic: model.TopicEvents, Key: key, Value: value}, nil
}

func publishEvent(ctx context.Context, client *kgo.Client, record *kgo.Record, timeout time.Duration) error {
	sendCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	attemptRecord := *record
	return client.ProduceSync(sendCtx, &attemptRecord).FirstErr()
}
