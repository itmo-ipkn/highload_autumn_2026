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
	UserCount       int
	EventsPerSec    int
	PoisonRate      float64
	DeliveryTimeout time.Duration
}

func LoadProducerConfig() (ProducerConfig, error) {
	cfg := ProducerConfig{
		Brokers:         Brokers(),
		ProducerID:      getEnvStr("PRODUCER_ID", ""),
		Topic:           model.TopicEvents,
		DLTTopic:        model.TopicDLT,
		KeyMode:         getEnvStr("KEY_MODE", ""),
		UserCount:       getEnvInt("USER_COUNT", 10),
		EventsPerSec:    getEnvInt("EVENTS_PER_SEC", 5),
		PoisonRate:      getEnvFloat("POISON_RATE", 0.02),
		DeliveryTimeout: time.Duration(getEnvInt("DELIVERY_TIMEOUT_MS", 25)) * time.Millisecond,
	}

	if cfg.KeyMode != model.KeyModeUserId && cfg.KeyMode != model.KeyModeNone {
		return ProducerConfig{}, fmt.Errorf("Invalid key mode: %s", cfg.KeyMode)
	}

	if len(cfg.Brokers) == 0 || cfg.ProducerID == "" || cfg.Topic == "" || cfg.DLTTopic == "" ||
		cfg.UserCount < 1 || cfg.EventsPerSec < 1 || cfg.PoisonRate < 0 || cfg.DeliveryTimeout < 0 {
		return ProducerConfig{}, fmt.Errorf(
			"Invalid environment variables: brokers=%v, producer_id=%q, topic=%q, dlt_topic=%q, key_mode=%q, user_count=%d, events_per_sec=%d, poison_rate=%v, delivery_timeout=%v",
			cfg.Brokers,
			cfg.ProducerID,
			cfg.Topic,
			cfg.DLTTopic,
			cfg.KeyMode,
			cfg.UserCount,
			cfg.EventsPerSec,
			cfg.PoisonRate,
			cfg.DeliveryTimeout,
		)
	}

	return cfg, nil
}
