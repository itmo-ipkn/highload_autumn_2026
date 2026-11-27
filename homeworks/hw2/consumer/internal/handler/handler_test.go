package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"highload/hw2/consumer/internal/event"
	"highload/hw2/consumer/internal/metrics"
	"highload/hw2/consumer/internal/sink"
)

func TestProcessRejectsMalformedEvent(t *testing.T) {
	h := &Handler{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	message := &sarama.ConsumerMessage{Value: []byte("{")}

	err := h.process(context.Background(), message)
	if err == nil || !strings.Contains(err.Error(), "decode event") {
		t.Fatalf("process() error = %v, want event decoding error", err)
	}
}

func TestProcessRejectsPoisonEventBeforeSink(t *testing.T) {
	h := &Handler{
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics: metrics.New("test"),
	}
	e := event.Event{
		EventID: uuid.New(), UserID: "user-1", Type: event.TypePurchase, Seq: 1,
		ProducedAt: time.Now().UTC(), Payload: json.RawMessage(`{"poison":true}`),
	}
	value, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}

	err = h.process(context.Background(), &sarama.ConsumerMessage{Value: value})
	if err == nil || !strings.Contains(err.Error(), "poison event") {
		t.Fatalf("process() error = %v, want poison event error", err)
	}
}

func TestProcessWithRetryCountsSuccessfulRetry(t *testing.T) {
	m := metrics.New("test")
	store := &fakeEventSink{failures: 1}
	publisher := &fakeDeadLetterPublisher{}
	h := &Handler{
		sink: store, dlt: publisher, metrics: m,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	if err := h.processWithRetry(context.Background(), validMessage(t)); err != nil {
		t.Fatalf("processWithRetry() error = %v, want nil", err)
	}
	if store.calls != 2 {
		t.Fatalf("sink calls = %d, want 2", store.calls)
	}
	if publisher.calls != 0 {
		t.Fatalf("DLT publish calls = %d, want 0", publisher.calls)
	}
	assertMetricValue(t, m, "consumer_processing_retries_total", "1")
}

func TestProcessWithRetryPublishesToDLTAfterThreeAttempts(t *testing.T) {
	m := metrics.New("test")
	store := &fakeEventSink{failures: processingAttempts}
	publisher := &fakeDeadLetterPublisher{}
	h := &Handler{
		sink: store, dlt: publisher, metrics: m,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	if err := h.processWithRetry(context.Background(), validMessage(t)); err != nil {
		t.Fatalf("processWithRetry() error = %v, want nil after DLT publish", err)
	}
	if store.calls != processingAttempts {
		t.Fatalf("sink calls = %d, want %d", store.calls, processingAttempts)
	}
	if publisher.calls != 1 || publisher.attempts != processingAttempts {
		t.Fatalf("DLT publish = (%d calls, %d attempts), want (1, %d)", publisher.calls, publisher.attempts, processingAttempts)
	}
	assertMetricValue(t, m, "consumer_processing_retries_total", "2")
	assertMetricValue(t, m, "consumer_events_dlt_total", "1")
}

func TestConsumeClaimDoesNotMarkMessageWhenDLTPublishFails(t *testing.T) {
	store := &fakeEventSink{failures: processingAttempts}
	publisher := &fakeDeadLetterPublisher{err: errors.New("broker unavailable")}
	h := &Handler{
		sink: store, dlt: publisher, commitInterval: time.Hour,
		metrics: metrics.New("test"),
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	session := &fakeConsumerGroupSession{ctx: context.Background()}
	messages := make(chan *sarama.ConsumerMessage, 1)
	messages <- validMessage(t)
	claim := &fakeConsumerGroupClaim{messages: messages}

	if err := h.ConsumeClaim(session, claim); err == nil {
		t.Fatal("ConsumeClaim() error = nil, want DLT publication error")
	}
	if len(session.marked) != 0 {
		t.Fatalf("marked messages = %d, want 0", len(session.marked))
	}
	if session.commits != 0 {
		t.Fatalf("commits = %d, want 0", session.commits)
	}
}

type fakeEventSink struct {
	calls    int
	failures int
}

func (s *fakeEventSink) Store(context.Context, sink.Record) (bool, error) {
	s.calls++
	if s.calls <= s.failures {
		return false, errors.New("temporary sink failure")
	}
	return true, nil
}

type fakeDeadLetterPublisher struct {
	calls    int
	attempts int
	err      error
}

func (p *fakeDeadLetterPublisher) Publish(_ *sarama.ConsumerMessage, _ error, attempts int) error {
	p.calls++
	p.attempts = attempts
	return p.err
}

type fakeConsumerGroupSession struct {
	ctx     context.Context
	marked  []*sarama.ConsumerMessage
	commits int
}

func (s *fakeConsumerGroupSession) Claims() map[string][]int32               { return nil }
func (s *fakeConsumerGroupSession) MemberID() string                         { return "test-member" }
func (s *fakeConsumerGroupSession) GenerationID() int32                      { return 1 }
func (s *fakeConsumerGroupSession) MarkOffset(string, int32, int64, string)  {}
func (s *fakeConsumerGroupSession) Commit()                                  { s.commits++ }
func (s *fakeConsumerGroupSession) ResetOffset(string, int32, int64, string) {}
func (s *fakeConsumerGroupSession) MarkMessage(message *sarama.ConsumerMessage, _ string) {
	s.marked = append(s.marked, message)
}
func (s *fakeConsumerGroupSession) Context() context.Context { return s.ctx }

type fakeConsumerGroupClaim struct {
	messages chan *sarama.ConsumerMessage
}

func (c *fakeConsumerGroupClaim) Topic() string              { return "events" }
func (c *fakeConsumerGroupClaim) Partition() int32           { return 0 }
func (c *fakeConsumerGroupClaim) InitialOffset() int64       { return 0 }
func (c *fakeConsumerGroupClaim) HighWaterMarkOffset() int64 { return 1 }
func (c *fakeConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return c.messages
}

func validMessage(t *testing.T) *sarama.ConsumerMessage {
	t.Helper()
	e := event.Event{
		EventID: uuid.New(), UserID: "user-1", Type: event.TypePurchase, Seq: 1,
		ProducedAt: time.Now().UTC(), Payload: json.RawMessage(`{"poison":false}`),
	}
	value, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return &sarama.ConsumerMessage{Topic: "events", Value: value}
}

func assertMetricValue(t *testing.T, m *metrics.Metrics, name, want string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler().ServeHTTP(recorder, request)
	for _, line := range strings.Split(recorder.Body.String(), "\n") {
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[1] == want {
			return
		}
	}
	t.Fatalf("metric %s with value %s not found in:\n%s", name, want, recorder.Body.String())
}
