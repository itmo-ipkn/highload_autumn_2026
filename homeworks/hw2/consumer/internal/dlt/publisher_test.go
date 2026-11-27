package dlt

import (
	"errors"
	"strconv"
	"testing"

	"github.com/IBM/sarama"
)

func TestPublishCopiesMessageAndAddsOriginHeaders(t *testing.T) {
	producer := &captureProducer{}
	publisher := &Publisher{producer: producer, topic: "events.dlt", podID: "consumer-2"}
	original := &sarama.ConsumerMessage{
		Topic: "events", Partition: 2, Offset: 42,
		Key: []byte("user-1"), Value: []byte(`{"event_id":"abc"}`),
	}
	cause := errors.New("poison event")

	if err := publisher.Publish(original, cause, 3); err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}
	if producer.message == nil {
		t.Fatal("SendMessage() was not called")
	}
	if producer.message.Topic != "events.dlt" {
		t.Errorf("message topic = %q, want events.dlt", producer.message.Topic)
	}
	if got := string(producer.message.Value.(sarama.ByteEncoder)); got != string(original.Value) {
		t.Errorf("message value = %q, want %q", got, original.Value)
	}
	if got := string(producer.message.Key.(sarama.ByteEncoder)); got != string(original.Key) {
		t.Errorf("message key = %q, want %q", got, original.Key)
	}

	gotHeaders := make(map[string]string, len(producer.message.Headers))
	for _, header := range producer.message.Headers {
		gotHeaders[string(header.Key)] = string(header.Value)
	}
	wantHeaders := map[string]string{
		"x-error":              cause.Error(),
		"x-original-topic":     original.Topic,
		"x-original-partition": strconv.FormatInt(int64(original.Partition), 10),
		"x-original-offset":    strconv.FormatInt(original.Offset, 10),
		"x-attempts":           "3",
		"x-consumer-pod":       "consumer-2",
	}
	for key, want := range wantHeaders {
		if gotHeaders[key] != want {
			t.Errorf("header %q = %q, want %q", key, gotHeaders[key], want)
		}
	}
}

type captureProducer struct {
	message *sarama.ProducerMessage
}

func (p *captureProducer) SendMessage(message *sarama.ProducerMessage) (int32, int64, error) {
	p.message = message
	return 0, 0, nil
}

func (p *captureProducer) Close() error { return nil }
