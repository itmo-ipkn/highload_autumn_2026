// consumer читает events в группе events-processor и пишет события в Postgres.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/consumer"
	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/env"
	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/kafkalog"
	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/sink"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("consumer", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	id := env.String("CONSUMER_ID", "1")
	pod := "consumer-" + id
	log = log.With("pod", pod)

	delay, err := env.Millis("PROCESSING_DELAY_MS", 15)
	if err != nil {
		return err
	}
	attempts, err := env.Int("MAX_ATTEMPTS", 3)
	if err != nil {
		return err
	}
	maxPoll, err := env.Int("MAX_POLL_RECORDS", 100)
	if err != nil {
		return err
	}
	session, err := env.Duration("SESSION_TIMEOUT", 15*time.Second)
	if err != nil {
		return err
	}
	commitInterval, err := env.Duration("COMMIT_INTERVAL", 2*time.Second)
	if err != nil {
		return err
	}
	topic := env.String("TOPIC", "events")
	dltTopic := env.String("DLT_TOPIC", "events.dlt")
	group := env.String("GROUP", "events-processor")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sink.Open(ctx, env.String("POSTGRES_DSN", "postgres://events:events@localhost:5432/events"))
	if err != nil {
		return err
	}
	defer db.Close()

	m := consumer.NewMetrics(env.Registerer(pod))
	env.ServeMetrics(env.String("METRICS_ADDR", ":2112"), log)

	owner, _ := strconv.ParseFloat(id, 64) // для метрики, у нечислового id будет 0
	g := &consumer.Group{
		Owner:          owner,
		CommitInterval: commitInterval,
		Topic:          topic,
		MaxPoll:        maxPoll,
		Offsets:        consumer.NewOffsets(),
		Metrics:        m,
		Log:            log,
	}
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(env.List("KAFKA_BROKERS", "localhost:9092")...),
		kgo.ClientID(pod),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		// Новая группа без коммитов начинает с начала топика: события, записанные до
		// старта консьюмеров, тоже обрабатываются. С latest они были бы пропущены.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.SessionTimeout(session),
		// Публикация в DLT тем же клиентом.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.WithLogger(&kafkalog.Logger{Out: log, MinLevel: kgo.LogLevelInfo}),
	}, g.Opts()...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}

	// нулевые серии consumer_events_total по всем партициям топика, см. Metrics.InitEvents
	if parts, err := partitions(ctx, cl, topic); err != nil {
		log.Warn("topic partitions, events series not initialized", "err", err)
	} else {
		m.InitEvents(parts)
	}

	g.Proc = &consumer.Processor{
		ConsumerID:   pod,
		Delay:        delay,
		MaxAttempts:  attempts,
		RetryBackoff: time.Second,
		Sink:         db,
		DLT:          consumer.KafkaDLT{Client: cl},
		DLTTopic:     dltTopic,
		Metrics:      m,
		Log:          log,
	}

	log.Info("start", "group", group, "topic", topic, "delay", delay.String(), "max_attempts", attempts)
	g.Run(ctx, cl)
	// Close уходит из группы: OnPartitionsRevoked коммитит то, что ещё не закоммичено.
	cl.Close()
	log.Info("stopped")
	return nil
}

// partitions - номера партиций топика из метаданных.
func partitions(ctx context.Context, cl *kgo.Client, topic string) ([]int32, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	td, err := kadm.NewClient(cl).ListTopics(ctx, topic)
	if err == nil {
		err = td.Error()
	}
	if err != nil {
		return nil, err
	}
	return td[topic].Partitions.Numbers(), nil
}
