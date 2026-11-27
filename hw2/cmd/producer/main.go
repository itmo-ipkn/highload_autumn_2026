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
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafka-events/internal/config"
	"kafka-events/internal/model"
)

var errShutdown = errors.New("shutdown requested")

func main() {
	cfg, err := config.LoadProducerConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(shutdown)

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ProducerID),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelInfo, func() string { return cfg.ProducerID })),
		kgo.RequiredAcks(kgo.AllISRAcks()),
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
	users := make([]string, cfg.UserCount)
	for i := range users {
		users[i] = fmt.Sprintf("%s-%s-user-%d", cfg.ProducerID, runID, i+1)
	}

	seqByUser := make([]int64, cfg.UserCount)
	interval := max(time.Second/time.Duration(cfg.EventsPerSec), time.Millisecond)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("producer started id=%s run_id=%s users=%d events_per_sec=%d key_mode=%s",
		cfg.ProducerID, runID, cfg.UserCount, cfg.EventsPerSec, cfg.KeyMode)

	producedCount := 0
	userIndex := 0

loop:
	for {
		select {
		case sig := <-shutdown:
			log.Printf("shutdown signal=%s producer_id=%s", sig, cfg.ProducerID)
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

			if err := publishWithRetry(shutdown, client, record, event, cfg.DeliveryTimeout); err != nil {
				if !errors.Is(err, errShutdown) {
					log.Printf("event delivery stopped event_id=%s user_id=%s seq=%d: %v",
						event.EventID, event.UserID, event.Seq, err)
				}
				break loop
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

func publishWithRetry(shutdown <-chan os.Signal, client *kgo.Client, record *kgo.Record, event model.Event, timeout time.Duration) error {
	backoff := 250 * time.Millisecond
	for {
		sendCtx, cancel := context.WithTimeout(context.Background(), timeout)
		attemptRecord := *record
		err := client.ProduceSync(sendCtx, &attemptRecord).FirstErr()
		cancel()
		if err == nil {
			return nil
		}

		log.Printf("kafka producer delivery failed, retrying event event_id=%s user_id=%s seq=%d err=%v",
			event.EventID, event.UserID, event.Seq, err)
		timer := time.NewTimer(backoff)
		select {
		case <-shutdown:
			timer.Stop()
			return errShutdown
		case <-timer.C:
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}
