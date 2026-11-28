package main

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Brokers         []string
	Topic           string
	GroupID         string
	ConsumerID      string
	ProcessingDelay int
	DLTTopic        string
}

func loadConfig() Config {
	return Config{
		Brokers:         strings.Split(getEnv("BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"), ","),
		Topic:           getEnv("TOPIC", "events"),
		GroupID:         getEnv("GROUP_ID", "events-processor"),
		ConsumerID:      getEnv("CONSUMER_ID", "consumer-1"),
		ProcessingDelay: getEnvInt("PROCESSING_DELAY_MS", 100),
		DLTTopic:        getEnv("DLT_TOPIC", "events.dlt"),
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}
