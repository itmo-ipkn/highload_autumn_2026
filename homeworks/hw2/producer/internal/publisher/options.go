package publisher

import (
	"time"

	"github.com/IBM/sarama"
)

// Options — параметры продюсера Kafka. Передаются одной структурой, без длинного списка аргументов.
type Options struct {
	Brokers  []string
	Topic    string
	Version  sarama.KafkaVersion
	ClientID string
	// PodID кладётся в заголовок x-producer-pod.
	PodID string
	// KeyByUser: true — ключ сообщения равен user_id, false — ключа нет (KEY_MODE=none).
	KeyByUser bool

	RetryMax        int
	RetryBackoff    time.Duration
	RetryBackoffMax time.Duration
	FlushFrequency  time.Duration
	FlushMessages   int
	MaxInFlight     int
	// ReportEvery — как часто логировать сводку (enqueued, acked, failed, in-flight). 0 — не логировать.
	ReportEvery time.Duration
}

// SaramaConfig собирает конфиг sarama для надёжной отправки.
func SaramaConfig(o Options) *sarama.Config {
	c := sarama.NewConfig()
	c.ClientID = o.ClientID
	c.Version = o.Version

	// Сообщение подтверждено, только когда его записали все реплики из ISR,
	// а их не меньше min.insync.replicas=2.
	c.Producer.RequiredAcks = sarama.WaitForAll
	// Идемпотентность: брокер отбрасывает дубли ретраев по (producer id, epoch, sequence)
	// и сохраняет порядок внутри партиции. Sarama требует для неё MaxOpenRequests=1.
	c.Producer.Idempotent = true
	c.Net.MaxOpenRequests = 1

	// Ретраи практически бесконечные, с экспоненциальным backoff. Пока кластер недоступен
	// (например, нет кворума ISR), события ждут в буфере, а не теряются. Буфер ограничен
	// MaxInFlight в Publisher.
	c.Producer.Retry.Max = o.RetryMax
	c.Producer.Retry.BackoffFunc = expBackoff(o.RetryBackoff, o.RetryBackoffMax)

	c.Producer.Return.Successes = true
	c.Producer.Return.Errors = true

	c.Producer.Partitioner = sarama.NewHashPartitioner // при nil-ключе партиция выбирается случайно
	c.Producer.Flush.Frequency = o.FlushFrequency
	c.Producer.Flush.Messages = o.FlushMessages
	c.Producer.Compression = sarama.CompressionLZ4

	c.Metadata.Retry.Max = 10
	c.Metadata.Retry.Backoff = 500 * time.Millisecond
	return c
}

func expBackoff(base, maxBackoff time.Duration) func(retries, maxRetries int) time.Duration {
	return func(retries, _ int) time.Duration {
		d := base
		for i := 1; i < retries && d < maxBackoff; i++ {
			d *= 2
		}
		return min(d, maxBackoff)
	}
}
