// Package config загружает конфигурацию продюсера из переменных окружения и .env файла.
package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/joho/godotenv"
)

// KeyMode определяет, что кладётся в ключ Kafka-сообщения.
type KeyMode string

const (
	// KeyModeUserID — ключ равен user_id. Все события пользователя попадают в одну партицию.
	KeyModeUserID KeyMode = "user_id"
	// KeyModeNone — ключа нет, sarama выбирает партицию случайно (эксперимент 4.3).
	KeyModeNone KeyMode = "none"
)

// Config — итоговая провалидированная конфигурация продюсера.
type Config struct {
	PodID string
	// RunID отличает запуски одного пода. После перезапуска под берёт новых пользователей,
	// поэтому seq старых пользователей не начинается заново с 1.
	RunID string

	Kafka     Kafka
	Generator Generator

	MetricsAddr     string
	ShutdownTimeout time.Duration
	LogLevel        string
}

type Kafka struct {
	Brokers      []string
	Topic        string
	Version      sarama.KafkaVersion
	KeyMode      KeyMode
	ClientID     string
	RetryMax     int
	RetryBackoff time.Duration
	// RetryBackoffMax — верхняя граница экспоненциального backoff между повторами.
	RetryBackoffMax time.Duration
	FlushFrequency  time.Duration
	FlushMessages   int
	// MaxInFlight ограничивает число отправленных, но ещё не подтверждённых сообщений.
	// Это backpressure: при недоступности брокеров генератор ждёт, а не копит события в памяти.
	MaxInFlight int
	// SaramaDebug включает внутренний лог sarama: подключения, метаданные, ретраи.
	SaramaDebug bool
}

type Generator struct {
	RatePerSec float64
	Burst      int
	Users      int
	Workers    int
	PoisonRate float64
	// MaxEvents — сколько событий сгенерировать и остановиться. 0 — без ограничения.
	MaxEvents uint64
}

// raw описывает переменные окружения. Дефолты рассчитаны на запуск в docker compose.
type raw struct {
	PodID string `env:"POD_ID" env-required:"true" env-description:"identifier of this producer pod"`
	RunID string `env:"RUN_ID" env-description:"run identifier; generated at start when empty"`

	Brokers         string        `env:"KAFKA_BROKERS" env-default:"kafka-1:19092,kafka-2:19092,kafka-3:19092"`
	Topic           string        `env:"KAFKA_TOPIC" env-default:"events"`
	KafkaVersion    string        `env:"KAFKA_VERSION" env-default:"4.0.0"`
	KeyMode         string        `env:"KEY_MODE" env-default:"user_id" env-description:"user_id|none"`
	RetryMax        int           `env:"PRODUCER_RETRY_MAX" env-default:"1000000"`
	RetryBackoff    time.Duration `env:"PRODUCER_RETRY_BACKOFF" env-default:"100ms"`
	RetryBackoffMax time.Duration `env:"PRODUCER_RETRY_BACKOFF_MAX" env-default:"2s"`
	FlushFrequency  time.Duration `env:"PRODUCER_FLUSH_FREQUENCY" env-default:"10ms"`
	FlushMessages   int           `env:"PRODUCER_FLUSH_MESSAGES" env-default:"100"`
	MaxInFlight     int           `env:"PRODUCER_MAX_IN_FLIGHT" env-default:"10000"`
	SaramaDebug     bool          `env:"SARAMA_DEBUG" env-default:"false"`

	RatePerSec float64 `env:"RATE_PER_SEC" env-default:"100"`
	Burst      int     `env:"RATE_BURST" env-default:"10"`
	Users      int     `env:"USERS_PER_PRODUCER" env-default:"20"`
	Workers    int     `env:"GENERATOR_WORKERS" env-default:"2"`
	PoisonRate float64 `env:"POISON_RATE" env-default:"0.02"`
	MaxEvents  uint64  `env:"MAX_EVENTS" env-default:"0"`

	MetricsAddr     string        `env:"METRICS_ADDR" env-default:":2112"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" env-default:"20s"`
	LogLevel        string        `env:"LOG_LEVEL" env-default:"info"`
}

var idPattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,32}$`)

// Load читает .env (путь из ENV_FILE, по умолчанию ./.env), затем окружение.
// Окружение важнее файла: godotenv.Load не перезаписывает уже заданные переменные,
// поэтому значения из docker compose всегда побеждают.
func Load() (Config, error) {
	envFile := os.Getenv("ENV_FILE")
	if envFile == "" {
		envFile = ".env"
	}
	if err := godotenv.Load(envFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return Config{}, fmt.Errorf("load %s: %w", envFile, err)
	}

	var r raw
	if err := cleanenv.ReadEnv(&r); err != nil {
		return Config{}, fmt.Errorf("read env: %w", err)
	}
	return fromRaw(r)
}

func fromRaw(r raw) (Config, error) {
	version, err := sarama.ParseKafkaVersion(r.KafkaVersion)
	if err != nil {
		return Config{}, fmt.Errorf("KAFKA_VERSION: %w", err)
	}
	runID := r.RunID
	if runID == "" {
		runID = strconv.FormatInt(time.Now().UnixNano(), 36)
	}

	cfg := Config{
		PodID: r.PodID,
		RunID: runID,
		Kafka: Kafka{
			Brokers:         splitList(r.Brokers),
			Topic:           r.Topic,
			Version:         version,
			KeyMode:         KeyMode(strings.ToLower(r.KeyMode)),
			ClientID:        "producer-" + r.PodID,
			RetryMax:        r.RetryMax,
			RetryBackoff:    r.RetryBackoff,
			RetryBackoffMax: r.RetryBackoffMax,
			FlushFrequency:  r.FlushFrequency,
			FlushMessages:   r.FlushMessages,
			MaxInFlight:     r.MaxInFlight,
			SaramaDebug:     r.SaramaDebug,
		},
		Generator: Generator{
			RatePerSec: r.RatePerSec,
			Burst:      r.Burst,
			Users:      r.Users,
			Workers:    r.Workers,
			PoisonRate: r.PoisonRate,
			MaxEvents:  r.MaxEvents,
		},
		MetricsAddr:     r.MetricsAddr,
		ShutdownTimeout: r.ShutdownTimeout,
		LogLevel:        r.LogLevel,
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate проверяет согласованность значений.
func (c Config) Validate() error {
	var errs []error
	if !idPattern.MatchString(c.PodID) {
		errs = append(errs, fmt.Errorf("POD_ID %q must match %s", c.PodID, idPattern))
	}
	if !idPattern.MatchString(c.RunID) {
		errs = append(errs, fmt.Errorf("RUN_ID %q must match %s", c.RunID, idPattern))
	}
	if len(c.Kafka.Brokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS must not be empty"))
	}
	if c.Kafka.Topic == "" {
		errs = append(errs, errors.New("KAFKA_TOPIC must not be empty"))
	}
	if c.Kafka.KeyMode != KeyModeUserID && c.Kafka.KeyMode != KeyModeNone {
		errs = append(errs, fmt.Errorf("KEY_MODE %q must be %q or %q", c.Kafka.KeyMode, KeyModeUserID, KeyModeNone))
	}
	if c.Kafka.RetryMax < 1 {
		errs = append(errs, errors.New("PRODUCER_RETRY_MAX must be >= 1 (required by idempotent producer)"))
	}
	if c.Kafka.RetryBackoff <= 0 || c.Kafka.RetryBackoffMax < c.Kafka.RetryBackoff {
		errs = append(errs, errors.New("need 0 < PRODUCER_RETRY_BACKOFF <= PRODUCER_RETRY_BACKOFF_MAX"))
	}
	if c.Kafka.FlushFrequency <= 0 || c.Kafka.FlushMessages <= 0 {
		errs = append(errs, errors.New("PRODUCER_FLUSH_FREQUENCY and PRODUCER_FLUSH_MESSAGES must be positive"))
	}
	if c.Kafka.MaxInFlight <= 0 {
		errs = append(errs, errors.New("PRODUCER_MAX_IN_FLIGHT must be positive"))
	}
	if c.Generator.RatePerSec <= 0 || c.Generator.Burst <= 0 {
		errs = append(errs, errors.New("RATE_PER_SEC and RATE_BURST must be positive"))
	}
	if c.Generator.Users <= 0 || c.Generator.Workers <= 0 {
		errs = append(errs, errors.New("USERS_PER_PRODUCER and GENERATOR_WORKERS must be positive"))
	}
	if c.Generator.PoisonRate < 0 || c.Generator.PoisonRate > 1 {
		errs = append(errs, errors.New("POISON_RATE must be within [0, 1]"))
	}
	if c.MetricsAddr == "" {
		errs = append(errs, errors.New("METRICS_ADDR must not be empty"))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("SHUTDOWN_TIMEOUT must be positive"))
	}
	return errors.Join(errs...)
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
