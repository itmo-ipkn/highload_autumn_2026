package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cfg := loadConfig()

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancel()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ConsumerID),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(500*time.Millisecond),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer client.Close()

	dsn := getEnv(
		"DATABASE_URL",
		"postgres://kafka:kafka_pass@postgres:5432/events_sink",
	)

	sink, err := NewSink(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer sink.Close(ctx)

	log.Printf(
		"consumer started id=%s group=%s topic=%s",
		cfg.ConsumerID,
		cfg.GroupID,
		cfg.Topic,
	)

	runConsumer(ctx, client, sink, cfg)
}
