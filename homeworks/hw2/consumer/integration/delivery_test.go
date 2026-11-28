// Run from the repository root with Docker running:
// go -C homeworks/hw2/consumer/integration test -v -count=1 -timeout=6m ./...
package integration_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"highload/hw2/consumer/internal/event"
	"highload/hw2/consumer/internal/sink"
)

func sample(seq uint64) event.Event {
	return event.Event{EventID: uuid.New(), UserID: "alice", Type: event.TypePurchase, Seq: seq, ProducedAt: time.Now().UTC().Truncate(time.Microsecond), Payload: json.RawMessage(`{}`)}
}

func (s *scenario) send(t *testing.T, e event.Event) int64 {
	t.Helper()
	value, err := json.Marshal(e)
	require.NoError(t, err)
	partition, offset, err := s.producer.SendMessage(&sarama.ProducerMessage{Topic: s.topic, Key: sarama.StringEncoder(e.UserID), Value: sarama.ByteEncoder(value)})
	require.NoError(t, err)
	require.Zero(t, partition)
	return offset
}

func (s *scenario) processed(t *testing.T, e event.Event, pod string, offset int64) {
	t.Helper()
	var count int
	err := s.db.QueryRowContext(t.Context(), `SELECT count(*) FROM processed_events WHERE event_id=$1 AND user_id=$2 AND seq=$3 AND event_type=$4 AND produced_at=$5 AND topic=$6 AND consumer_pod=$7 AND kafka_partition=0 AND kafka_offset=$8`, e.EventID, e.UserID, e.Seq, e.Type, e.ProducedAt, s.topic, pod, offset).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count, "processed event %s at %s[0]:%d by %s", e.EventID, s.topic, offset, pod)
}

func (s *scenario) counts(t *testing.T, processed, duplicates int) {
	t.Helper()
	var p, d int
	err := s.db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM processed_events WHERE topic=$1), (SELECT count(*) FROM duplicate_deliveries WHERE topic=$1)`, s.topic).Scan(&p, &d)
	require.NoError(t, err)
	require.Equal(t, processed, p, "processed_events")
	require.Equal(t, duplicates, d, "duplicate_deliveries")
}

func TestDelivery(t *testing.T) {
	c := startCluster(t)
	t.Run("normal", func(t *testing.T) {
		s := c.scenario(t, "normal")
		s.consume(t, s.handler(t, "first", c.sink), false)
		e := sample(1)
		offset := s.send(t, e)
		s.committed(t, offset+1)
		s.processed(t, e, "first", offset)
		s.counts(t, 1, 0)
		s.deadCount(t, 0)
	})
	t.Run("duplicate", func(t *testing.T) {
		s := c.scenario(t, "duplicate")
		s.consume(t, s.handler(t, "first", c.sink), false)
		e := sample(1)
		first := s.send(t, e)
		second := s.send(t, e)
		require.Equal(t, first+1, second)
		s.committed(t, second+1)
		s.processed(t, e, "first", first)
		s.counts(t, 1, 1)
		var id, pod string
		var partition int32
		var offset int64
		err := s.db.QueryRowContext(t.Context(), `SELECT event_id, consumer_pod, kafka_partition, kafka_offset FROM duplicate_deliveries WHERE topic=$1`, s.topic).Scan(&id, &pod, &partition, &offset)
		require.NoError(t, err)
		require.Equal(t, e.EventID.String(), id)
		require.Equal(t, "first", pod)
		require.Zero(t, partition)
		require.Equal(t, second, offset)
		s.deadCount(t, 0)
	})
	t.Run("poison", func(t *testing.T) {
		s := c.scenario(t, "poison")
		s.consume(t, s.handler(t, "first", c.sink), false)
		// Use a nonzero source offset: a default-valued x-original-offset must not pass.
		s.send(t, sample(1))
		e := sample(2)
		e.Payload = json.RawMessage(`{"poison":true}`)
		offset := s.send(t, e)
		s.committed(t, offset+1)
		s.deadCount(t, 1)
		s.counts(t, 1, 0)
		var count int
		require.NoError(t, s.db.QueryRowContext(t.Context(), `SELECT count(*) FROM processed_events WHERE event_id=$1`, e.EventID).Scan(&count))
		require.Zero(t, count)
		reader, err := sarama.NewConsumerFromClient(c.client)
		require.NoError(t, err)
		closeAfter(t, reader)
		pc, err := reader.ConsumePartition(s.dead, 0, 0)
		require.NoError(t, err)
		closeAfter(t, pc)
		select {
		case msg := <-pc.Messages():
			require.NotNil(t, msg)
			value, err := json.Marshal(e)
			require.NoError(t, err)
			require.Equal(t, value, msg.Value)
			require.Equal(t, e.UserID, string(msg.Key))
			headers := map[string]string{}
			for _, h := range msg.Headers {
				headers[string(h.Key)] = string(h.Value)
			}
			require.Contains(t, headers["x-error"], "poison event "+e.EventID.String())
			for key, want := range map[string]string{
				"x-original-topic":     s.topic,
				"x-original-partition": "0",
				"x-original-offset":    strconv.FormatInt(offset, 10),
				"x-attempts":           "3",
				"x-consumer-pod":       "first",
			} {
				require.Equal(t, want, headers[key], key)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("no DLT record")
		}
	})
	t.Run("rebalance", func(t *testing.T) { testRebalance(t, c.scenario(t, "rebalance")) })
}

// Park only the unprocessed record before the first owner's SQL write. Kafka's
// actual session cancellation (not a test timer) interrupts it on revocation.
type gatedStore struct {
	*sink.Sink
	pending uuid.UUID
	entered chan struct{}
}

func (g gatedStore) Store(ctx context.Context, r sink.Record) (bool, error) {
	if r.Event.EventID != g.pending {
		return g.Sink.Store(ctx, r)
	}
	select {
	case g.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return false, ctx.Err()
}

type observedHandler struct {
	sarama.ConsumerGroupHandler
	start chan int64
}

func (h observedHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	select {
	case h.start <- claim.InitialOffset():
	case <-session.Context().Done():
		return nil
	}
	return h.ConsumerGroupHandler.ConsumeClaim(session, claim)
}

func testRebalance(t *testing.T, s *scenario) {
	first, pending := sample(1), sample(2)
	gate := gatedStore{s.sink, pending.EventID, make(chan struct{}, 1)}
	s.consume(t, s.handler(t, "old", gate), true)
	acked := s.send(t, first)
	s.committed(t, acked+1)
	next := s.send(t, pending)
	require.Equal(t, acked+1, next)
	select {
	case <-gate.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("old owner never received pending record")
	}
	s.committed(t, next)
	s.processed(t, first, "old", acked)
	s.counts(t, 1, 0)
	// Joining the same group revokes the old session while the SQL write is pending.
	start := make(chan int64, 1)
	s.consume(t, observedHandler{s.handler(t, "new", s.sink), start}, false)
	select {
	case initial := <-start:
		require.Equal(t, next, initial, "new owner's initial offset")
	case <-time.After(45 * time.Second):
		t.Fatal("partition not transferred")
	}
	s.committed(t, next+1)
	s.processed(t, first, "old", acked)
	s.processed(t, pending, "new", next)
	s.counts(t, 2, 0)
	s.deadCount(t, 0)
}
