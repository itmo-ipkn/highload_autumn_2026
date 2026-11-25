// producer пишет события пользователей своего пула в топик events.
package main

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/env"
	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/event"
	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/kafkalog"
	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/producer"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("producer", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	id := env.String("PRODUCER_ID", "1")
	pod := "producer-" + id
	log = log.With("pod", pod)

	users, err := env.Int("USERS_PER_PRODUCER", 50)
	if err != nil {
		return err
	}
	rate, err := env.Float("RATE", 30)
	if err != nil {
		return err
	}
	poison, err := env.Float("POISON_RATE", 0.02)
	if err != nil {
		return err
	}
	mode, err := producer.ParseKeyMode(env.String("KEY_MODE", "user_id"))
	if err != nil {
		return err
	}
	deliveryTimeout, err := env.Duration("DELIVERY_TIMEOUT", 5*time.Minute)
	if err != nil {
		return err
	}
	requestTimeout, err := env.Duration("PRODUCE_REQUEST_TIMEOUT", 10*time.Second)
	if err != nil {
		return err
	}
	flushTimeout, err := env.Duration("FLUSH_TIMEOUT", 20*time.Second)
	if err != nil {
		return err
	}

	reg := env.Registerer(pod)
	m := producer.NewMetrics(reg)
	env.ServeMetrics(env.String("METRICS_ADDR", ":2112"), log)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(env.List("KAFKA_BROKERS", "localhost:9092")...),
		kgo.ClientID(pod),
		kgo.DefaultProduceTopic(env.String("TOPIC", "events")),
		// acks=all: запись подтверждена, когда её получили все реплики из ISR,
		// а ISR не меньше min.insync.replicas топика.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Идемпотентность в franz-go включена по умолчанию (kgo.DisableIdempotentWrite не
		// вызываем): producer id + sequence на партицию, брокер отбрасывает повторы батчей
		// после ретраев, порядок внутри партиции сохраняется.
		kgo.RecordDeliveryTimeout(deliveryTimeout),
		kgo.ProduceRequestTimeout(requestTimeout),
		kgo.WithLogger(&kafkalog.Logger{
			Out:          log,
			MinLevel:     kgo.LogLevelInfo,
			OnBatchError: func(code string) { m.BatchErrors.WithLabelValues(code).Inc() },
		}),
	)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	gen := event.NewGenerator(id, users, poison, rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0)))
	log.Info("start", "users", users, "rate", rate, "poison_rate", poison, "key_mode", mode,
		"delivery_timeout", deliveryTimeout.String())

	var st producer.Stats
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				log.Info("stats", "generated", st.Generated.Load(), "acked", st.Acked.Load(),
					"failed", st.Failed.Load(), "buffered", cl.BufferedProduceRecords())
			}
		}
	}()

	producer.Run(ctx, producer.Config{Rate: rate, KeyMode: mode, FlushTimeout: flushTimeout}, gen, cl, m, log, &st)
	cl.Close()
	log.Info("stopped", "generated", st.Generated.Load(), "acked", st.Acked.Load(), "failed", st.Failed.Load())
	return nil
}
