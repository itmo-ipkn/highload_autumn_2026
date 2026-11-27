package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/jackc/pgx/v5/stdlib"

	"highload/hw2/consumer/internal/verify"
)

func main() {
	login := env("GITHUB_LOGIN", "ekubyshin")
	fmt.Printf("%s %s\n", login, time.Now().Format(time.RFC3339))

	version, err := sarama.ParseKafkaVersion(env("KAFKA_VERSION", "4.0.0"))
	if err != nil {
		log.Fatalf("parse KAFKA_VERSION: %v", err)
	}
	cfg := verify.Config{
		Brokers:       split(env("KAFKA_BROKERS", "kafka-1:19092,kafka-2:19092,kafka-3:19092")),
		KafkaVersion:  version,
		EventsTopic:   env("KAFKA_TOPIC", "events"),
		DLTTopic:      env("DLT_TOPIC", "events.dlt"),
		ConsumerGroup: env("KAFKA_CONSUMER_GROUP", "events-processor"),
		PostgresURL:   env("POSTGRES_URL", "postgres://events:events@postgres:5432/events?sslmode=disable"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result, err := verify.Run(ctx, cfg, log.Default())
	if err != nil {
		log.Fatalf("verify failed: %v", err)
	}
	if !result.Passed() {
		fmt.Printf("FAIL: missing sequences=%d, poison not in DLT=%d, non-poison in DLT=%d, order violations=%d\n",
			len(result.MissingSequences), len(result.PoisonNotInDLT), len(result.NonPoisonInDLT), result.OrderViolations)
		printExamples("missing", result.MissingSequences)
		printExamples("poison_not_in_dlt", result.PoisonNotInDLT)
		printExamples("non_poison_in_dlt", result.NonPoisonInDLT)
		os.Exit(1)
	}
	fmt.Printf("PASS: no missing sequences; all poison events are in DLT; no ordering violations\n")
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
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

func printExamples(name string, values []string) {
	const limit = 10
	for i, value := range values {
		if i == limit {
			fmt.Printf("%s: ... and %d more\n", name, len(values)-limit)
			return
		}
		fmt.Printf("%s: %s\n", name, value)
	}
}
