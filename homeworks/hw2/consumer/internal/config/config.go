// Package config loads and validates consumer configuration.
package config

import (
	"errors"
	"fmt"
	"math"
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

type VerifyConfig struct {
	GitHubLogin   string
	Brokers       []string
	EventsTopic   string
	DLTTopic      string
	ConsumerGroup string
	KafkaVersion  sarama.KafkaVersion
	PostgresURL   string
}

type connectionEnv struct {
	Brokers      string `env:"KAFKA_BROKERS" env-default:"kafka-1:19092,kafka-2:19092,kafka-3:19092"`
	Topic        string `env:"KAFKA_TOPIC" env-default:"events"`
	DLTTopic     string `env:"DLT_TOPIC" env-default:"events.dlt"`
	Group        string `env:"KAFKA_CONSUMER_GROUP" env-default:"events-processor"`
	KafkaVersion string `env:"KAFKA_VERSION" env-default:"4.0.0"`
	PostgresURL  string `env:"POSTGRES_URL" env-required:"true"`
}

type raw struct {
	Connection        connectionEnv
	PodID             string        `env:"POD_ID" env-required:"true"`
	ProcessingDelayMS int64         `env:"PROCESSING_DELAY_MS" env-default:"0"`
	CommitInterval    time.Duration `env:"COMMIT_INTERVAL" env-default:"1s"`
}

func readEnv(dst any) error {
	path := os.Getenv("ENV_FILE")
	if path == "" {
		path = ".env"
	}
	if err := godotenv.Load(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("load %s: %w", path, err)
	}
	if err := cleanenv.ReadEnv(dst); err != nil {
		return fmt.Errorf("read environment: %w", err)
	}
	return nil
}

func (r connectionEnv) parse() (Config, error) {
	version, err := sarama.ParseKafkaVersion(r.KafkaVersion)
	if err != nil {
		return Config{}, fmt.Errorf("KAFKA_VERSION: %w", err)
	}
	cfg := Config{
		Brokers: split(r.Brokers), Topic: strings.TrimSpace(r.Topic), DLTTopic: strings.TrimSpace(r.DLTTopic), Group: strings.TrimSpace(r.Group),
		KafkaVersion: version, PostgresURL: strings.TrimSpace(r.PostgresURL),
	}
	return cfg, nil
}

// LoadVerify loads shared connection settings without requiring a consumer pod ID.
// The login is available for the CLI banner even if connection validation fails.
func LoadVerify() (VerifyConfig, error) {
	var r struct {
		Connection  connectionEnv
		GitHubLogin string `env:"GITHUB_LOGIN" env-default:"ekubyshin"`
	}
	err := readEnv(&r)
	cfg := VerifyConfig{GitHubLogin: strings.TrimSpace(r.GitHubLogin)}
	if err != nil {
		return cfg, err
	}
	c, err := r.Connection.parse()
	if err != nil {
		return cfg, err
	}
	if err := c.validateConnection(); err != nil {
		return cfg, err
	}
	cfg.Brokers, cfg.KafkaVersion, cfg.PostgresURL = c.Brokers, c.KafkaVersion, c.PostgresURL
	cfg.EventsTopic, cfg.DLTTopic, cfg.ConsumerGroup = c.Topic, c.DLTTopic, c.Group
	return cfg, nil
}

func Load() (Config, error) {
	var r raw
	if err := readEnv(&r); err != nil {
		return Config{}, err
	}
	cfg, err := r.Connection.parse()
	if err != nil {
		return Config{}, err
	}
	if r.ProcessingDelayMS < 0 || r.ProcessingDelayMS > math.MaxInt64/int64(time.Millisecond) {
		return Config{}, errors.New("PROCESSING_DELAY_MS outside duration range")
	}
	cfg.PodID, cfg.CommitInterval = r.PodID, r.CommitInterval
	cfg.ProcessingDelay = time.Duration(r.ProcessingDelayMS) * time.Millisecond
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validateConnection() error {
	var errs []error
	if len(c.Brokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS must not be empty"))
	}
	if c.Topic == "" || c.DLTTopic == "" || c.Group == "" {
		errs = append(errs, errors.New("KAFKA_TOPIC, DLT_TOPIC, and KAFKA_CONSUMER_GROUP are required"))
	}
	if c.PostgresURL == "" {
		errs = append(errs, errors.New("POSTGRES_URL is required"))
	}
	return errors.Join(errs...)
}

func (c Config) Validate() error {
	errs := []error{c.validateConnection()}
	if strings.TrimSpace(c.PodID) == "" {
		errs = append(errs, errors.New("POD_ID is required"))
	}
	if c.ProcessingDelay < 0 || c.CommitInterval <= 0 {
		errs = append(errs, errors.New("PROCESSING_DELAY_MS must be non-negative and COMMIT_INTERVAL positive"))
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
