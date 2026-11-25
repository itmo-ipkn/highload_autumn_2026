package kafkalog

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestErrCode(t *testing.T) {
	cases := map[string]error{
		"NOT_ENOUGH_REPLICAS":      kerr.NotEnoughReplicas,
		"NOT_LEADER_FOR_PARTITION": fmt.Errorf("wrapped: %w", kerr.NotLeaderForPartition),
		"other":                    errors.New("dial tcp: refused"),
		"none":                     nil,
	}
	for want, err := range cases {
		if got := ErrCode(err); got != want {
			t.Errorf("ErrCode(%v) = %s, want %s", err, got, want)
		}
	}
}

func TestBatchErrorHook(t *testing.T) {
	var codes []string
	l := &Logger{
		Out:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		MinLevel:     kgo.LogLevelInfo,
		OnBatchError: func(c string) { codes = append(codes, c) },
	}
	l.Log(kgo.LogLevelInfo, BatchFailedMsg, "broker", "1", "err", kerr.NotEnoughReplicas, "partition", 2)
	l.Log(kgo.LogLevelInfo, "metadata update", "err", kerr.NotEnoughReplicas)
	if len(codes) != 1 || codes[0] != "NOT_ENOUGH_REPLICAS" {
		t.Fatalf("codes %v", codes)
	}
}

func TestParseRetries(t *testing.T) {
	update := "events[0{1234=>1240}, 1{retrying@-1,12(NOT_ENOUGH_REPLICAS: Messages are rejected since there are fewer in-sync replicas than required.)}, " +
		"2{retrying@-1,3(REQUEST_TIMED_OUT: The request timed out.)}, ], events.dlt[1{retrying@-1,1(NOT_ENOUGH_REPLICAS: x)}, ]"
	got := ParseRetries(update)
	want := []Retry{
		{"events", "1", "12", "NOT_ENOUGH_REPLICAS"},
		{"events", "2", "3", "REQUEST_TIMED_OUT"},
		{"events.dlt", "1", "1", "NOT_ENOUGH_REPLICAS"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %+v, want %+v", got[i], want[i])
		}
	}
}

func TestRetriesCountedAndDebugHidden(t *testing.T) {
	var codes []string
	var out strings.Builder
	l := &Logger{
		Out:          slog.New(slog.NewTextHandler(&out, nil)),
		MinLevel:     kgo.LogLevelInfo,
		OnBatchError: func(c string) { codes = append(codes, c) },
	}
	if l.Level() != kgo.LogLevelDebug {
		t.Fatal("debug needed to see retries")
	}
	l.Log(kgo.LogLevelDebug, "produced", "broker", "2", "to", "events[0{retrying@-1,5(NOT_ENOUGH_REPLICAS: x)}, ]")
	l.Log(kgo.LogLevelDebug, "some debug noise")
	if len(codes) != 1 || codes[0] != "NOT_ENOUGH_REPLICAS" {
		t.Fatalf("codes %v", codes)
	}
	if strings.Contains(out.String(), "noise") || !strings.Contains(out.String(), "code=NOT_ENOUGH_REPLICAS") {
		t.Fatalf("output:\n%s", out.String())
	}
}
