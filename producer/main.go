package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	Brokers    []string
	Topic      string
	ProducerID string
	UserStart  int
	UserEnd    int
	Rate       int
	KeyMode    string
}

func loadConfig() (Config, error) {
	cfg := Config{
		Brokers:    strings.Split(getEnv("BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"), ","),
		Topic:      getEnv("TOPIC", "events"),
		ProducerID: getEnv("PRODUCER_ID", "producer-1"),
		KeyMode:    getEnv("KEY_MODE", "user_id"),
	}

	var err error

	cfg.UserStart, err = getIntEnv("USER_ID_START", 1)
	if err != nil {
		return cfg, err
	}

	cfg.UserEnd, err = getIntEnv("USER_ID_END", 100)
	if err != nil {
		return cfg, err
	}

	cfg.Rate, err = getIntEnv("RATE", 5)
	if err != nil {
		return cfg, err
	}

	if cfg.UserStart > cfg.UserEnd {
		return cfg, fmt.Errorf("USER_ID_START must be <= USER_ID_END")
	}

	if cfg.Rate <= 0 {
		return cfg, fmt.Errorf("RATE must be positive")
	}

	if cfg.KeyMode != "user_id" && cfg.KeyMode != "none" {
		return cfg, fmt.Errorf("KEY_MODE must be user_id or none")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getIntEnv(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	return strconv.Atoi(value)
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ProducerID),
		kgo.ProducerBatchMaxBytes(1_000_000),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		log.Fatalf("failed to create Kafka client: %v", err)
	}
	defer client.Close()

	log.Printf(
		"producer started: id=%s users=%d-%d rate=%d key_mode=%s brokers=%v",
		cfg.ProducerID,
		cfg.UserStart,
		cfg.UserEnd,
		cfg.Rate,
		cfg.KeyMode,
		cfg.Brokers,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runProducer(ctx, client, cfg)
	log.Printf("producer stopped: %s", cfg.ProducerID)
}

func runProducer(ctx context.Context, client *kgo.Client, cfg Config) {
	userCount := cfg.UserEnd - cfg.UserStart + 1
	seq := make([]int, userCount)

	interval := time.Second / time.Duration(cfg.Rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	userIndex := 0

	for {
		select {
		case <-ctx.Done():
			log.Printf("shutdown signal received, stopping generation")
			return
		case <-ticker.C:
		}

		userID := fmt.Sprintf("user_%d", cfg.UserStart+userIndex)
		seq[userIndex]++

		event, err := generateEvent(userID, seq[userIndex])
		if err != nil {
			seq[userIndex]--
			log.Printf("event generation failed for %s: %v", userID, err)
			continue
		}

		err = sendEvent(ctx, client, cfg, event)
		if err != nil {
			seq[userIndex]--
			log.Printf("event send failed for user=%s seq=%d: %v", userID, event.Seq, err)
		} else {
			log.Printf("sent event user=%s seq=%d type=%s poison=%t",
				event.UserID, event.Seq, event.Type, isPoison(event))
		}

		userIndex = (userIndex + 1) % userCount
	}
}

func sendEvent(ctx context.Context, client *kgo.Client, cfg Config, event Event) error {
	value, err := marshalEvent(event)
	if err != nil {
		return err
	}

	var key []byte
	if cfg.KeyMode == "user_id" {
		key = []byte(event.UserID)
	}

	record := &kgo.Record{
		Topic: cfg.Topic,
		Key:   key,
		Value: value,
	}

	backoff := time.Second

	for {
		err := client.ProduceSync(ctx, record).FirstErr()
		if err == nil {
			return nil
		}

		log.Printf("Kafka send error producer=%s user=%s seq=%d: %v; retrying in %s",
			cfg.ProducerID, event.UserID, event.Seq, err, backoff)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func marshalEvent(event Event) ([]byte, error) {
	return json.Marshal(event)
}

func isPoison(event Event) bool {
	var payload struct {
		Poison bool `json:"poison"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return false
	}
	return payload.Poison
}
