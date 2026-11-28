package config

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestFromRawValidatesConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*raw)
		wantErr bool
	}{
		{
			name: "valid defaults",
		},
		{
			name: "invalid pod id",
			mutate: func(r *raw) {
				r.PodID = "bad/id"
			},
			wantErr: true,
		},
		{
			name: "unsupported key mode",
			mutate: func(r *raw) {
				r.KeyMode = "random"
			},
			wantErr: true,
		},
		{
			name: "invalid poison rate",
			mutate: func(r *raw) {
				r.PoisonRate = 1.1
			},
			wantErr: true,
		},
		{
			name: "invalid kafka version",
			mutate: func(r *raw) {
				r.KafkaVersion = "not-a-version"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := raw{
				PodID: "p1", RunID: "run1",
				Brokers: "kafka-1:19092, kafka-2:19092", Topic: "events",
				KafkaVersion: "4.0.0", KeyMode: string(KeyModeUserID),
				RetryMax: 3, RetryBackoff: 100 * time.Millisecond, RetryBackoffMax: time.Second,
				FlushFrequency: 10 * time.Millisecond, FlushMessages: 100, MaxInFlight: 100,
				RatePerSec: 10, Burst: 2, Users: 2, Workers: 1, PoisonRate: 0.02,
				MetricsAddr: ":2112", ShutdownTimeout: time.Second,
			}
			if tt.mutate != nil {
				tt.mutate(&r)
			}
			got, err := fromRaw(r)
			if (err != nil) != tt.wantErr {
				t.Fatalf("fromRaw() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if got.PodID != "p1" || got.RunID != "run1" {
					t.Fatalf("identity = (%q, %q), want (p1, run1)", got.PodID, got.RunID)
				}
				if len(got.Kafka.Brokers) != 2 || got.Kafka.Version != sarama.V4_0_0_0 {
					t.Fatalf("Kafka config = %+v", got.Kafka)
				}
			}
		})
	}
}

func TestSplitListTrimsAndDropsEmptyValues(t *testing.T) {
	got := splitList(" broker-1, ,broker-2 ")
	if len(got) != 2 || got[0] != "broker-1" || got[1] != "broker-2" {
		t.Fatalf("splitList() = %#v", got)
	}
}
