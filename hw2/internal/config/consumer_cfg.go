package config

import (
	"fmt"
	"kafka-events/internal/model"
	"time"
)

const (
	maxRetries = 3
	groupID    = "events-processor"
)

type ConsumerConfig struct {
	Brokers          []string
	GroupID          string
	ConsumerID       string
	Topic            string
	DLTTopic         string
	PgURL            string
	ProcessingDelay  time.Duration
	ProcessingJitter time.Duration
	PollRecords      int
	MaxRetries       int
}

func LoadConsumerConfig() (ConsumerConfig, error) {
	cfg := ConsumerConfig{
		Brokers:          Brokers(),
		GroupID:          groupID,
		ConsumerID:       getEnvStr("CONSUMER_ID", ""),
		Topic:            model.TopicEvents,
		DLTTopic:         model.TopicDLT,
		PgURL:            getEnvStr("DATABASE_URL", ""),
		ProcessingDelay:  time.Duration(getEnvInt("PROCESSING_DELAY_MS", 100)) * time.Millisecond,
		ProcessingJitter: time.Duration(getEnvInt("PROCESSING_JITTER_MS", 0)) * time.Millisecond,
		PollRecords:      getEnvInt("POLL_RECORDS", 1),
		MaxRetries:       maxRetries,
	}

	if len(cfg.Brokers) == 0 || cfg.PgURL == "" || cfg.GroupID == "" || cfg.ConsumerID == "" ||
		cfg.Topic == "" || cfg.DLTTopic == "" || cfg.PollRecords < 0 || cfg.MaxRetries < 0 ||
		cfg.ProcessingDelay < 0 || cfg.ProcessingJitter < 0 {
		return ConsumerConfig{}, fmt.Errorf(
			"Invalid environment variables: brokers=%v, pg_url=%q, group_id=%q, consumer_id=%q,"+
				"topic=%q, dlt_topic=%q, poll_records=%d, max_retries=%d,"+
				"processing_delay=%v, processing_jitter=%v",
			cfg.Brokers,
			cfg.PgURL,
			cfg.GroupID,
			cfg.ConsumerID,
			cfg.Topic,
			cfg.DLTTopic,
			cfg.PollRecords,
			cfg.MaxRetries,
			cfg.ProcessingDelay,
			cfg.ProcessingJitter,
		)
	}

	return cfg, nil
}
