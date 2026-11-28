package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"

	"highload/hw2/consumer/internal/config"
	"highload/hw2/consumer/internal/dlt"
	"highload/hw2/consumer/internal/handler"
	"highload/hw2/consumer/internal/metrics"
	"highload/hw2/consumer/internal/sink"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("consumer stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	m := metrics.New(cfg.PodID)
	stopMetrics := startMetrics(m, logger)
	defer stopMetrics()

	return runGroup(ctx, cfg, m, logger)
}

func startMetrics(m *metrics.Metrics, logger *slog.Logger) func() {
	metricsCtx, stopMetrics := context.WithCancel(context.Background())
	metricsDone := make(chan error, 1)
	go func() { metricsDone <- metrics.Serve(metricsCtx, ":2113", m.Handler()) }()
	return func() {
		stopMetrics()
		if err := <-metricsDone; err != nil {
			logger.Error("metrics server", slog.Any("error", err))
		}
	}
}

func runGroup(ctx context.Context, cfg config.Config, m *metrics.Metrics, logger *slog.Logger) error {
	dbCtx, cancelDB := context.WithTimeout(ctx, 10*time.Second)
	eventSink, err := sink.Open(dbCtx, cfg.PostgresURL)
	cancelDB()
	if err != nil {
		return err
	}
	defer func() {
		if err := eventSink.Close(); err != nil {
			logger.Error("close postgres", slog.Any("error", err))
		}
	}()

	dltPublisher, err := dlt.Open(cfg.Brokers, cfg.KafkaVersion, cfg.DLTTopic, cfg.PodID)
	if err != nil {
		return err
	}
	defer func() {
		if err := dltPublisher.Close(); err != nil {
			logger.Error("close DLT producer", slog.Any("error", err))
		}
	}()

	group, err := openGroup(cfg)
	if err != nil {
		return err
	}
	defer func() {
		if err := group.Close(); err != nil {
			logger.Error("close consumer group", slog.Any("error", err))
		}
	}()

	consumerHandler := handler.New(cfg, eventSink, dltPublisher, logger, m)
	logger.Info("consumer started", slog.String("pod_id", cfg.PodID), slog.String("group", cfg.Group), slog.String("topic", cfg.Topic))
	consumeSessions(ctx, group, cfg.Topic, consumerHandler, logger)
	return nil
}

func openGroup(cfg config.Config) (sarama.ConsumerGroup, error) {
	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Version = cfg.KafkaVersion
	kafkaCfg.Consumer.Return.Errors = true
	kafkaCfg.Consumer.Offsets.AutoCommit.Enable = false
	kafkaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	kafkaCfg.Consumer.Group.Session.Timeout = 10 * time.Second
	kafkaCfg.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	if err := kafkaCfg.Validate(); err != nil {
		return nil, err
	}
	return sarama.NewConsumerGroup(cfg.Brokers, cfg.Group, kafkaCfg)
}

func consumeSessions(ctx context.Context, group sarama.ConsumerGroup, topic string, consumerHandler sarama.ConsumerGroupHandler, logger *slog.Logger) {
	go func() {
		for err := range group.Errors() {
			logger.Error("consumer group error", slog.Any("error", err))
		}
	}()

	for ctx.Err() == nil {
		if err := group.Consume(ctx, []string{topic}, consumerHandler); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("consume cycle failed", slog.Any("error", err))
			timer := time.NewTimer(time.Second)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
			}
		}
	}
}
