package handler

import (
	"bytes"
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

func TestConsumeClaimPublishesPoisonAfterThreeAttempts(t *testing.T) {
	m := metrics.New("test")
	store := &fakeEventSink{}
	publisher := &fakeDeadLetterPublisher{}
	h := &Handler{
		sink: store, dlt: publisher, metrics: m,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	h.commitInterval = time.Hour
	msg := messageWithPayload(t, `{"poison":true}`)
	session := &fakeConsumerGroupSession{ctx: t.Context()}
	if err := h.ConsumeClaim(session, newClaim(msg)); err != nil {
		t.Fatal(err)
	}
	if store.calls != 0 || len(session.marked) != 1 || session.marked[0] != msg {
		t.Fatalf("sink calls = %d, marked = %v; want poison marked without sink write", store.calls, session.marked)
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
	messages <- messageWithPayload(t, `{"poison":true}`)
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

func newClaim(messages ...*sarama.ConsumerMessage) *fakeConsumerGroupClaim {
	ch := make(chan *sarama.ConsumerMessage, len(messages))
	for _, msg := range messages {
		ch <- msg
	}
	close(ch)
	return &fakeConsumerGroupClaim{messages: ch}
}

func TestConsumeClaimRetriesUnmarkedOffsetAfterRecovery(t *testing.T) {
	store := &fakeEventSink{failures: processingAttempts}
	publisher := &fakeDeadLetterPublisher{}
	var logs bytes.Buffer
	h := &Handler{podID: "pod-1", sink: store, dlt: publisher, commitInterval: time.Hour,
		metrics: metrics.New("test"), logger: slog.New(slog.NewTextHandler(&logs, nil))}
	first, next := messageWithPayload(t, `"text"`), messageWithPayload(t, `[1,2]`)
	first.Offset, next.Offset = 42, 43
	failed := &fakeConsumerGroupSession{ctx: t.Context()}
	if err := h.ConsumeClaim(failed, newClaim(first, next)); err == nil || len(failed.marked) != 0 || store.calls != processingAttempts {
		t.Fatalf("failed claim: err=%v, marked=%v, sink calls=%d", err, failed.marked, store.calls)
	}
	replay := &fakeConsumerGroupSession{ctx: t.Context()}
	if err := h.ConsumeClaim(replay, newClaim(first, next)); err != nil {
		t.Fatal(err)
	}
	if len(replay.marked) != 2 || replay.marked[0] != first || replay.marked[1] != next || publisher.calls != 0 || store.calls != processingAttempts+2 {
		t.Fatalf("replayed claim: marked=%v, DLT calls=%d, sink calls=%d", replay.marked, publisher.calls, store.calls)
	}
	if strings.Count(logs.String(), `msg="first claim message"`) != 2 || strings.Count(logs.String(), `msg="first claim message" pod_id=pod-1 topic=events partition=0 offset=42`) != 2 {
		t.Errorf("unexpected claim logs: %s", &logs)
	}
}

func TestConsumeClaimCanceledDuringDLTSend(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	publisher := &fakeDeadLetterPublisher{send: func(sendCtx context.Context) error {
		close(started)
		<-sendCtx.Done()
		return sendCtx.Err()
	}}
	h := &Handler{dlt: publisher, commitInterval: time.Hour, metrics: metrics.New("test"), logger: slog.New(slog.DiscardHandler)}
	session := &fakeConsumerGroupSession{ctx: ctx}
	claim := newClaim(messageWithPayload(t, `{"poison":true}`))
	done := make(chan error, 1)
	go func() { done <- h.ConsumeClaim(session, claim) }()
	select {
	case <-started:
		cancel()
	case <-time.After(3 * time.Second):
		t.Fatal("DLT send did not start")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || len(session.marked) != 0 {
			t.Fatalf("canceled DLT send: err=%v, marked=%v", err, session.marked)
		}
	case <-time.After(time.Second):
		t.Fatal("claim stuck after session cancellation")
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
	send     func(context.Context) error
	calls    int
	attempts int
	err      error
}

func (p *fakeDeadLetterPublisher) Publish(ctx context.Context, _ *sarama.ConsumerMessage, _ error, attempts int) error {
	p.calls++
	p.attempts = attempts
	if p.send != nil {
		return p.send(ctx)
	}
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
	return messageWithPayload(t, `{"poison":false}`)
}

func messageWithPayload(t *testing.T, payload string) *sarama.ConsumerMessage {
	t.Helper()
	e := event.Event{
		EventID: uuid.New(), UserID: "user-1", Type: event.TypePurchase, Seq: 1,
		ProducedAt: time.Now().UTC(), Payload: json.RawMessage(payload),
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
