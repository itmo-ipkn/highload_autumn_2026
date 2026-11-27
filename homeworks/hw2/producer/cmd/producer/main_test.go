package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"highload/hw2/producer/internal/generator"
	"highload/hw2/producer/internal/publisher"
)

// Каналы результатов контролирует тест: AsyncClose начинает, но не завершает drain.
type delayedProducer struct {
	sarama.AsyncProducer
	input    chan *sarama.ProducerMessage
	acks     chan *sarama.ProducerMessage
	failures chan *sarama.ProducerError
	closing  chan struct{}
}

func (p *delayedProducer) Input() chan<- *sarama.ProducerMessage     { return p.input }
func (p *delayedProducer) Successes() <-chan *sarama.ProducerMessage { return p.acks }
func (p *delayedProducer) Errors() <-chan *sarama.ProducerError      { return p.failures }
func (p *delayedProducer) AsyncClose()                               { close(p.closing) }

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for producer")
		var zero T
		return zero
	}
}

func startTestProducer(t *testing.T, users []string, timeout time.Duration) (*delayedProducer, *publisher.Publisher, *generator.Generator, context.CancelFunc, <-chan error) {
	t.Helper()
	broker := &delayedProducer{
		input: make(chan *sarama.ProducerMessage, 2), acks: make(chan *sarama.ProducerMessage),
		failures: make(chan *sarama.ProducerError), closing: make(chan struct{}),
	}
	logger := slog.New(slog.DiscardHandler)
	pub, err := publisher.NewBuilder(publisher.Options{Topic: "events", MaxInFlight: 2}).
		WithProducer(broker).WithLogger(logger).Build()
	if err != nil {
		t.Fatal(err)
	}
	gen, err := generator.New(pub, users, generator.WithWorkers(2), generator.WithUnlimitedRate(), generator.WithSeed(42))
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(t.Context())
	t.Cleanup(stop)
	done := make(chan error, 1)
	go func() { done <- runProducer(ctx, gen, pub, logger, timeout) }()
	return broker, pub, gen, stop, done
}

func TestDeliveryFailureStartsDrainWhileAnotherACKIsPending(t *testing.T) {
	broker, pub, _, _, done := startTestProducer(t, generator.UserIDs("pod", "run", 32), time.Second)
	first, second := receive(t, broker.input), receive(t, broker.input)
	wantErr := errors.New("terminal delivery failure")
	broker.failures <- &sarama.ProducerError{Msg: first, Err: wantErr}
	receive(t, broker.closing) // Без SIGTERM и до ответа на второе сообщение.
	broker.acks <- second
	close(broker.acks)
	close(broker.failures)
	if err := receive(t, done); !errors.Is(err, wantErr) {
		t.Fatalf("runProducer error = %v, want %v", err, wantErr)
	}
	if got := pub.Stats(); got != (publisher.Stats{Enqueued: 2, Acked: 1, Failed: 1}) {
		t.Fatalf("unexpected stats: %+v", got)
	}
}

func TestProducerDrain(t *testing.T) {
	deliveryErr := errors.New("broker rejected message")
	for _, tc := range []struct {
		name    string
		wantErr error
		stats   publisher.Stats
	}{
		{"late ACK", nil, publisher.Stats{Enqueued: 1, Acked: 1}},
		{"late error", deliveryErr, publisher.Stats{Enqueued: 1, Failed: 1}},
		{"no response", context.DeadlineExceeded, publisher.Stats{Enqueued: 1, InFlight: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {

			timeout := time.Second
			if tc.stats.InFlight != 0 {
				timeout = 20 * time.Millisecond
			}
			broker, pub, gen, stop, done := startTestProducer(t, []string{"u"}, timeout)
			msg := receive(t, broker.input)
			stop()
			receive(t, broker.closing)
			if tc.stats.InFlight == 0 {
				if tc.wantErr == nil {
					broker.acks <- msg
				} else {
					broker.failures <- &sarama.ProducerError{Msg: msg, Err: tc.wantErr}
				}
				close(broker.acks)
				close(broker.failures)
			}
			if err := receive(t, done); !errors.Is(err, tc.wantErr) {
				t.Fatalf("runProducer error = %v, want %v", err, tc.wantErr)
			}
			if got := pub.Stats(); got != tc.stats {
				t.Fatalf("stats = %+v, want %+v", got, tc.stats)
			}
			if got := gen.Sequencer().Last("u"); got != tc.stats.Acked {
				t.Fatalf("last seq = %d, want %d", got, tc.stats.Acked)
			}
			if tc.stats.InFlight != 0 {
				broker.acks <- msg
				close(broker.acks)
				close(broker.failures)
			}
		})
	}
}
