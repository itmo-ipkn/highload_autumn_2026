package config

import (
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestConfigValidate(t *testing.T) {
	valid := Config{
		PodID: "consumer-1", Brokers: []string{"kafka-1:19092"},
		Topic: "events", DLTTopic: "events.dlt", Group: "events-processor",
		PostgresURL:     "postgres://test:example@postgres/events?sslmode=disable",
		ProcessingDelay: 10 * time.Millisecond, CommitInterval: time.Second,
	}

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{name: "valid"},
		{name: "missing pod", mutate: func(c *Config) { c.PodID = " " }, wantErr: true},
		{name: "missing brokers", mutate: func(c *Config) { c.Brokers = nil }, wantErr: true},
		{name: "missing topic", mutate: func(c *Config) { c.DLTTopic = "" }, wantErr: true},
		{name: "missing postgres", mutate: func(c *Config) { c.PostgresURL = "" }, wantErr: true},
		{name: "negative delay", mutate: func(c *Config) { c.ProcessingDelay = -time.Millisecond }, wantErr: true},
		{name: "zero commit interval", mutate: func(c *Config) { c.CommitInterval = 0 }, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			cfg.Brokers = append([]string(nil), valid.Brokers...)
			if tt.mutate != nil {
				tt.mutate(&cfg)
			}
			if err := cfg.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProcessingDelayMS(t *testing.T) {
	t.Setenv("ENV_FILE", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("POD_ID", "test")
	t.Setenv("POSTGRES_URL", "postgres://test")
	t.Setenv("PROCESSING_DELAY_MS", "50")
	cfg, err := Load()
	if err != nil || cfg.ProcessingDelay != 50*time.Millisecond {
		t.Fatalf("processing delay = %v, err = %v", cfg.ProcessingDelay, err)
	}
	for _, invalid := range []string{"-1", "50ms", "9223372036854775807"} {
		t.Setenv("PROCESSING_DELAY_MS", invalid)
		if _, err := Load(); err == nil {
			t.Errorf("accepted invalid PROCESSING_DELAY_MS=%s", invalid)
		}
	}
}

func TestLoadVerifyOnlyNeedsSharedEnv(t *testing.T) {
	t.Setenv("ENV_FILE", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("POD_ID", "")
	t.Setenv("PROCESSING_DELAY_MS", "unused")
	t.Setenv("KAFKA_BROKERS", " a:123, ,b:456 ")
	t.Setenv("KAFKA_TOPIC", "source")
	t.Setenv("DLT_TOPIC", "dead")
	t.Setenv("KAFKA_CONSUMER_GROUP", "workers")
	t.Setenv("KAFKA_VERSION", "3.6.0")
	t.Setenv("POSTGRES_URL", "postgres://test")
	t.Setenv("GITHUB_LOGIN", "test-login")
	cfg, err := LoadVerify()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cfg.Brokers, []string{"a:123", "b:456"}) || cfg.EventsTopic != "source" || cfg.DLTTopic != "dead" || cfg.ConsumerGroup != "workers" || cfg.KafkaVersion != sarama.V3_6_0_0 || cfg.PostgresURL != "postgres://test" || cfg.GitHubLogin != "test-login" {
		t.Fatal("verify did not load configured connection and login")
	}
	t.Setenv("KAFKA_VERSION", "invalid")
	if cfg, err := LoadVerify(); err == nil || cfg.GitHubLogin != "test-login" {
		t.Fatal("invalid Kafka version must fail without losing the CLI banner login")
	}
}

func TestSplitTrimsAndDropsEmptyValues(t *testing.T) {
	got := split(" broker-1, ,broker-2 ")
	if len(got) != 2 || got[0] != "broker-1" || got[1] != "broker-2" {
		t.Fatalf("split() = %#v", got)
	}
}
