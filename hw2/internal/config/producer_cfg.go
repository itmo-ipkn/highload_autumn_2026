package config

import (
	"fmt"
	"kafka-events/internal/model"
	"time"
)

type ProducerConfig struct {
	Brokers         []string
	ProducerID      string
	Topic           string
	DLTTopic        string
	KeyMode         string
	UsersCount      int
	RetriesCount    int
	EventsPerSec    int
	PoisonRate      float64
	RequestTimeout  time.Duration
	DeliveryTimeout time.Duration
}

func LoadProducerConfig() (ProducerConfig, error) {
	cfg := ProducerConfig{
		Brokers:         Brokers(),
		ProducerID:      getEnvStr("PRODUCER_ID", ""),
		Topic:           model.TopicEvents,
		DLTTopic:        model.TopicDLT,
		KeyMode:         getEnvStr("KEY_MODE", ""),
		UsersCount:      getEnvInt("USERS_COUNT", 10),
		RetriesCount:    getEnvInt("RETRIES_COUNT", 5),
		EventsPerSec:    getEnvInt("EVENTS_PER_SEC", 5),
		PoisonRate:      getEnvFloat("POISON_RATE", 0.02),
		RequestTimeout:  time.Duration(getEnvInt("REQUEST_TIMEOUT_MS", 250)) * time.Millisecond,
		DeliveryTimeout: time.Duration(getEnvInt("DELIVERY_TIMEOUT_MS", 500)) * time.Millisecond,
	}

	if cfg.KeyMode != model.KeyModeUserId && cfg.KeyMode != model.KeyModeNone {
		return ProducerConfig{}, fmt.Errorf("Invalid key mode: %s", cfg.KeyMode)
	}

	if len(cfg.Brokers) == 0 || cfg.ProducerID == "" || cfg.Topic == "" || cfg.DLTTopic == "" || cfg.RetriesCount < 0 ||
		cfg.UsersCount < 1 || cfg.EventsPerSec < 1 || cfg.PoisonRate < 0 || cfg.RequestTimeout < 1 || cfg.DeliveryTimeout < 1 {
		return ProducerConfig{}, fmt.Errorf(
			"Invalid environment variables: brokers=%v, producer_id=%q, topic=%q, dlt_topic=%q, key_mode=%q, "+
				"user_count=%d, retries_count=%d, events_per_sec=%d, poison_rate=%v, request_timeout=%v delivery_timeout=%v",
			cfg.Brokers,
			cfg.ProducerID,
			cfg.Topic,
			cfg.DLTTopic,
			cfg.KeyMode,
			cfg.UsersCount,
			cfg.RetriesCount,
			cfg.EventsPerSec,
			cfg.PoisonRate,
			cfg.RequestTimeout,
			cfg.DeliveryTimeout,
		)
	}

	return cfg, nil
}
