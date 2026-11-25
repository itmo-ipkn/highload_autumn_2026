package consumer

import (
	"context"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Заголовки записи в DLT.
const (
	HeaderError             = "error"
	HeaderOriginalTopic     = "original-topic"
	HeaderOriginalPartition = "original-partition"
	HeaderOriginalOffset    = "original-offset"
	HeaderAttempts          = "attempts"
	HeaderConsumer          = "consumer"
)

// DLTRecord - запись для DLT: тот же ключ и value, что у исходной, причина ошибки и
// координаты исходной записи в заголовках.
func DLTRecord(r *kgo.Record, topic string, cause error, attempts int, consumerID string) *kgo.Record {
	return &kgo.Record{
		Topic: topic,
		Key:   r.Key,
		Value: r.Value,
		Headers: []kgo.RecordHeader{
			{Key: HeaderError, Value: []byte(cause.Error())},
			{Key: HeaderOriginalTopic, Value: []byte(r.Topic)},
			{Key: HeaderOriginalPartition, Value: []byte(strconv.Itoa(int(r.Partition)))},
			{Key: HeaderOriginalOffset, Value: []byte(strconv.FormatInt(r.Offset, 10))},
			{Key: HeaderAttempts, Value: []byte(strconv.Itoa(attempts))},
			{Key: HeaderConsumer, Value: []byte(consumerID)},
		},
	}
}

// Header - значение заголовка или "".
func Header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// KafkaDLT публикует в DLT через клиент консьюмера (acks=all, идемпотентный продюсер).
type KafkaDLT struct {
	Client *kgo.Client
}

// Publish отправляет запись и ждёт подтверждения брокера.
func (d KafkaDLT) Publish(ctx context.Context, r *kgo.Record) error {
	return d.Client.ProduceSync(ctx, r).FirstErr()
}
