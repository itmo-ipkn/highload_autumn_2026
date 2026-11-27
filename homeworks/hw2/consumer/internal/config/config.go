// Package config loads and validates consumer configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/joho/godotenv"
)

type Config struct {
	PodID           string
	Brokers         []string
	Topic           string
	DLTTopic        string
	Group           string
	KafkaVersion    sarama.KafkaVersion
	PostgresURL     string
	ProcessingDelay time.Duration
	CommitInterval  time.Duration
}

type raw struct {
	PodID           string        `env:"POD_ID" env-required:"true"`
	Brokers         string        `env:"KAFKA_BROKERS" env-default:"kafka-1:19092,kafka-2:19092,kafka-3:19092"`
	Topic           string        `env:"KAFKA_TOPIC" env-default:"events"`
	DLTTopic        string        `env:"DLT_TOPIC" env-default:"events.dlt"`
	Group           string        `env:"KAFKA_CONSUMER_GROUP" env-default:"events-processor"`
	KafkaVersion    string        `env:"KAFKA_VERSION" env-default:"4.0.0"`
	PostgresURL     string        `env:"POSTGRES_URL" env-default:"postgres://events:events@postgres:5432/events?sslmode=disable"`
	ProcessingDelay time.Duration `env:"PROCESSING_DELAY" env-default:"0ms"`
	CommitInterval  time.Duration `env:"COMMIT_INTERVAL" env-default:"1s"`
}

func Load() (Config, error) {
	path := os.Getenv("ENV_FILE")
	if path == "" {
		path = ".env"
	}
	if err := godotenv.Load(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return Config{}, fmt.Errorf("load %s: %w", path, err)
	}
	var r raw
	if err := cleanenv.ReadEnv(&r); err != nil {
		return Config{}, fmt.Errorf("read environment: %w", err)
	}
	version, err := sarama.ParseKafkaVersion(r.KafkaVersion)
	if err != nil {
		return Config{}, fmt.Errorf("KAFKA_VERSION: %w", err)
	}
	cfg := Config{
		PodID: r.PodID, Brokers: split(r.Brokers), Topic: r.Topic, DLTTopic: r.DLTTopic, Group: r.Group,
		KafkaVersion: version, PostgresURL: r.PostgresURL,
		ProcessingDelay: r.ProcessingDelay, CommitInterval: r.CommitInterval,
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	var errs []error
	if strings.TrimSpace(c.PodID) == "" {
		errs = append(errs, errors.New("POD_ID is required"))
	}
	if len(c.Brokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS must not be empty"))
	}
	if c.Topic == "" || c.DLTTopic == "" || c.Group == "" {
		errs = append(errs, errors.New("KAFKA_TOPIC, DLT_TOPIC, and KAFKA_CONSUMER_GROUP are required"))
	}
	if c.PostgresURL == "" {
		errs = append(errs, errors.New("POSTGRES_URL is required"))
	}
	if c.ProcessingDelay < 0 || c.CommitInterval <= 0 {
		errs = append(errs, errors.New("PROCESSING_DELAY must be non-negative and COMMIT_INTERVAL positive"))
	}
	return errors.Join(errs...)
}

func split(value string) []string {
	var values []string
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			values = append(values, part)
		}
	}
	return values
}
