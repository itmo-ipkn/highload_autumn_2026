package main

import (
	"bytes"
	"context"
	"log"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"highload/hw2/consumer/internal/config"
	"highload/hw2/consumer/internal/verify"
)

func TestRun(t *testing.T) {
	t.Setenv("ENV_FILE", t.TempDir()+"/absent")
	t.Setenv("GITHUB_LOGIN", "test-login")
	t.Setenv("POSTGRES_URL", "postgres://localhost/hw2")
	t.Setenv("KAFKA_BROKERS", "one:9092,two:9092")
	t.Setenv("KAFKA_VERSION", "4.0.0")
	t.Setenv("KAFKA_TOPIC", "source")
	t.Setenv("DLT_TOPIC", "dlt")
	t.Setenv("KAFKA_CONSUMER_GROUP", "custom-group")
	for _, tc := range []struct {
		name   string
		result verify.Result
		err    error
		code   int
	}{
		{"pass", verify.Result{}, nil, 0},
		{"only order", verify.Result{OrderViolations: 4, DuplicateDeliveries: 3}, nil, 2},
		{"technical error", verify.Result{OrderViolations: 1}, context.DeadlineExceeded, 1},
		{"missing seq", verify.Result{MissingSequences: []string{"a:1"}}, nil, 1},
		{"order and missing seq", verify.Result{OrderViolations: 2, MissingSequences: []string{"a:1"}}, nil, 1},
		{"order and loss", verify.Result{OrderViolations: 2, MissingSinkEvents: []string{"lost"}}, nil, 1},
		{"order and conflict", verify.Result{OrderViolations: 2, SequenceConflicts: []string{"a:1"}}, nil, 1},
		{"order and missing poison", verify.Result{OrderViolations: 2, PoisonNotInDLT: []string{"lost"}}, nil, 1},
		{"order and misrouted event", verify.Result{OrderViolations: 2, NonPoisonInDLT: []string{"ordinary"}}, nil, 1},
		{"order and lag", verify.Result{OrderViolations: 2, ConsumerLag: 1}, nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			code := run(&out, log.New(&out, "", 0), func(_ context.Context, cfg config.VerifyConfig, _ *log.Logger) (verify.Result, error) {
				if !slices.Equal(cfg.Brokers, []string{"one:9092", "two:9092"}) || cfg.KafkaVersion != sarama.V4_0_0_0 || cfg.EventsTopic != "source" || cfg.DLTTopic != "dlt" || cfg.ConsumerGroup != "custom-group" || cfg.PostgresURL != "postgres://localhost/hw2" {
					t.Fatalf("unexpected verify config: %+v", cfg)
				}
				return tc.result, tc.err
			})
			if code != tc.code {
				t.Fatalf("code = %d, want %d: %s", code, tc.code, &out)
			}
			assertBanner(t, out.String(), "test-login")
		})
	}
}

func TestRunConfigErrorKeepsBanner(t *testing.T) {
	t.Setenv("ENV_FILE", t.TempDir()+"/absent")
	t.Setenv("KAFKA_VERSION", "invalid")
	t.Setenv("GITHUB_LOGIN", "custom-login")
	var out bytes.Buffer
	if code := run(&out, log.New(&out, "", 0), nil); code != 1 {
		t.Fatalf("code for configuration error = %d, want 1", code)
	}
	assertBanner(t, out.String(), "custom-login")
}

func assertBanner(t *testing.T, output, wantLogin string) {
	t.Helper()
	line, _, ok := strings.Cut(output, "\n")
	login, stamp, space := strings.Cut(line, " ")
	if !ok || !space || login != wantLogin {
		t.Fatalf("first line: %q", line)
	}
	if _, err := time.Parse(time.RFC3339, stamp); err != nil {
		t.Fatalf("first line: %q: %v", line, err)
	}
}
