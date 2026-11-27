// Command producer генерирует события пользователей этого пода и публикует их в Kafka.
// Здесь только сборка зависимостей и управление жизненным циклом. Вся логика в internal/.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"

	"highload/hw2/producer/internal/config"
	"highload/hw2/producer/internal/generator"
	"highload/hw2/producer/internal/metrics"
	"highload/hw2/producer/internal/publisher"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "producer: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger := newLogger(cfg.LogLevel).With(slog.String("pod", cfg.PodID), slog.String("run_id", cfg.RunID))
	slog.SetDefault(logger)
	if cfg.Kafka.SaramaDebug {
		sarama.Logger = log.New(os.Stderr, "[sarama] ", log.LstdFlags|log.Lmicroseconds)
	}

	// SIGTERM останавливает генерацию. Сервер метрик живёт до конца flush,
	// чтобы на графиках было видно, как досылается буфер.
	sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	m := metrics.New(cfg.PodID)
	srvCtx, stopServer := context.WithCancel(context.Background())
	srvDone := make(chan error, 1)
	go func() { srvDone <- metrics.Serve(srvCtx, cfg.MetricsAddr, m.Handler()) }()
	defer func() {
		stopServer()
		if err := <-srvDone; err != nil {
			logger.Error("metrics server", slog.Any("err", err))
		}
	}()

	pub, err := connect(sigCtx, logger, publisher.NewBuilder(publisherOptions(cfg)).
		WithLogger(logger.With(slog.String("component", "publisher"))).
		WithMetrics(m))
	if err != nil {
		return err
	}

	users := generator.UserIDs(cfg.PodID, cfg.RunID, cfg.Generator.Users)
	gen, err := generator.New(pub, users,
		generator.WithRate(cfg.Generator.RatePerSec, cfg.Generator.Burst),
		generator.WithWorkers(cfg.Generator.Workers),
		generator.WithPoisonRate(cfg.Generator.PoisonRate),
		generator.WithMaxEvents(cfg.Generator.MaxEvents),
		generator.WithLogger(logger.With(slog.String("component", "generator"))),
		generator.WithMetrics(m),
	)
	if err != nil {
		return err
	}

	logger.Info("producer started",
		slog.String("topic", cfg.Kafka.Topic),
		slog.String("key_mode", string(cfg.Kafka.KeyMode)),
		slog.Float64("rate_per_sec", cfg.Generator.RatePerSec),
		slog.Int("users", len(users)),
		slog.String("first_user", users[0]),
		slog.Float64("poison_rate", cfg.Generator.PoisonRate),
		slog.Uint64("max_events", cfg.Generator.MaxEvents),
	)

	genErr := gen.Run(sigCtx)
	if genErr == nil && sigCtx.Err() == nil {
		logger.Info("MAX_EVENTS reached, generation stopped; waiting for SIGTERM")
		<-sigCtx.Done()
	}
	if genErr != nil {
		logger.Error("generator stopped with error", slog.Any("err", genErr))
	}

	logger.Info("shutting down: flushing buffered messages", slog.Int("in_flight", pub.Stats().InFlight))
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelFlush()
	closeErr := pub.Close(flushCtx)

	logFinalStats(logger, pub.Stats(), gen.Sequencer().Snapshot())
	if closeErr != nil {
		logger.Error("flush incomplete", slog.Any("err", closeErr))
	}
	return errors.Join(genErr, closeErr)
}

// connect ждёт доступности Kafka: при старте стенда брокеры могут ещё выбирать лидеров.
func connect(ctx context.Context, logger *slog.Logger, b *publisher.Builder) (*publisher.Publisher, error) {
	const attempts = 30
	var lastErr error
	for i := 1; i <= attempts; i++ {
		pub, err := b.Build()
		if err == nil {
			return pub, nil
		}
		lastErr = err
		logger.Warn("kafka is not ready, retrying", slog.Int("attempt", i), slog.Any("err", err))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, lastErr
}

func publisherOptions(cfg config.Config) publisher.Options {
	return publisher.Options{
		Brokers:         cfg.Kafka.Brokers,
		Topic:           cfg.Kafka.Topic,
		Version:         cfg.Kafka.Version,
		ClientID:        cfg.Kafka.ClientID,
		PodID:           cfg.PodID,
		KeyByUser:       cfg.Kafka.KeyMode == config.KeyModeUserID,
		RetryMax:        cfg.Kafka.RetryMax,
		RetryBackoff:    cfg.Kafka.RetryBackoff,
		RetryBackoffMax: cfg.Kafka.RetryBackoffMax,
		FlushFrequency:  cfg.Kafka.FlushFrequency,
		FlushMessages:   cfg.Kafka.FlushMessages,
		MaxInFlight:     cfg.Kafka.MaxInFlight,
		ReportEvery:     10 * time.Second,
	}
}

// logFinalStats печатает итог: сколько отправлено и последний seq каждого пользователя.
// В норме enqueued == acked == сумма last_seq, а failed == 0.
func logFinalStats(logger *slog.Logger, s publisher.Stats, seqs []generator.UserSeq) {
	var total uint64
	parts := make([]string, 0, len(seqs))
	for _, us := range seqs {
		total += us.LastSeq
		parts = append(parts, fmt.Sprintf("%s=%d", us.UserID, us.LastSeq))
	}
	logger.Info("producer stopped",
		slog.Uint64("enqueued", s.Enqueued),
		slog.Uint64("acked", s.Acked),
		slog.Uint64("failed", s.Failed),
		slog.Int("unacked", s.InFlight),
		slog.Uint64("seq_total", total),
		slog.String("last_seq", strings.Join(parts, ",")),
	)
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
