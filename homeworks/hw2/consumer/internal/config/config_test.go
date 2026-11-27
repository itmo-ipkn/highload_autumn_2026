package config

import (
	"testing"
	"time"
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

func TestSplitTrimsAndDropsEmptyValues(t *testing.T) {
	got := split(" broker-1, ,broker-2 ")
	if len(got) != 2 || got[0] != "broker-1" || got[1] != "broker-2" {
		t.Fatalf("split() = %#v", got)
	}
}
