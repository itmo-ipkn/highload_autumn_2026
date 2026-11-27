package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"highload/hw2/consumer/internal/config"
	"highload/hw2/consumer/internal/dlt"
	"highload/hw2/consumer/internal/handler"
	"highload/hw2/consumer/internal/metrics"
	"highload/hw2/consumer/internal/sink"
)

type cluster struct {
	brokers  []string
	db       *sql.DB
	sink     *sink.Sink
	admin    sarama.ClusterAdmin
	producer sarama.SyncProducer
	client   sarama.Client
}

func closeAfter(t *testing.T, c io.Closer) {
	t.Helper()
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("close %T: %v", c, err)
		}
	})
}

func kafkaConfig() *sarama.Config {
	c := sarama.NewConfig()
	c.Version = sarama.V3_5_0_0
	c.Consumer.Offsets.AutoCommit.Enable = false
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	c.Consumer.Group.Heartbeat.Interval = time.Second
	c.Producer.Return.Successes = true
	return c
}

func startCluster(t *testing.T) *cluster {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	k, err := kafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	tc.CleanupContainer(t, k)
	require.NoError(t, err)
	brokers, err := k.Brokers(ctx)
	require.NoError(t, err)
	pg, err := postgres.Run(ctx, "postgres:16-alpine", postgres.WithInitScripts("../../deploy/postgres/init.sql"), postgres.BasicWaitStrategies())
	tc.CleanupContainer(t, pg)
	require.NoError(t, err)
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	store, err := sink.Open(ctx, dsn)
	require.NoError(t, err)
	closeAfter(t, store)
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	closeAfter(t, db)
	client, err := sarama.NewClient(brokers, kafkaConfig())
	require.NoError(t, err)
	closeAfter(t, client)
	admin, err := sarama.NewClusterAdmin(brokers, kafkaConfig())
	require.NoError(t, err)
	closeAfter(t, admin)
	producer, err := sarama.NewSyncProducerFromClient(client)
	require.NoError(t, err)
	closeAfter(t, producer)
	return &cluster{brokers: brokers, db: db, sink: store, admin: admin, producer: producer, client: client}
}

type scenario struct {
	*cluster
	topic, dead, group string
}

func (c *cluster) scenario(t *testing.T, name string) *scenario {
	t.Helper()
	s := &scenario{c, name, name + ".dlt", name + "-group"}
	for _, topic := range []string{s.topic, s.dead} {
		require.NoError(t, c.admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false))
	}
	return s
}

type eventStore interface {
	Store(context.Context, sink.Record) (bool, error)
}

func (s *scenario) handler(t *testing.T, pod string, store eventStore) *handler.Handler {
	t.Helper()
	pub, err := dlt.Open(s.brokers, kafkaConfig().Version, s.dead, pod)
	require.NoError(t, err)
	closeAfter(t, pub)
	return handler.New(config.Config{PodID: pod, CommitInterval: 50 * time.Millisecond}, store, pub, slog.Default(), metrics.New(pod))
}

// A one-session member leaves the group on revocation rather than rejoining it.
func (s *scenario) consume(t *testing.T, h sarama.ConsumerGroupHandler, oneSession bool) {
	t.Helper()
	g, err := sarama.NewConsumerGroup(s.brokers, s.group, kafkaConfig())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if err := g.Close(); err != nil {
				t.Errorf("leave %s: %v", s.group, err)
			}
		}()
		for ctx.Err() == nil {
			if err := g.Consume(ctx, []string{s.topic}, h); err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("consume %s: %v", s.topic, err)
				return
			}
			if oneSession {
				return
			}
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
}

func (s *scenario) committed(t *testing.T, want int64) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		r, err := s.admin.ListConsumerGroupOffsets(s.group, map[string][]int32{s.topic: {0}})
		require.NoError(c, err)
		b := r.GetBlock(s.topic, 0)
		require.NotNil(c, b)
		require.Equal(c, sarama.ErrNoError, b.Err)
		require.Equal(c, want, b.Offset)
	}, 30*time.Second, 50*time.Millisecond, "committed next offset for %s[0]", s.topic)
}

func (s *scenario) deadCount(t *testing.T, want int64) {
	t.Helper()
	end, err := s.client.GetOffset(s.dead, 0, sarama.OffsetNewest)
	require.NoError(t, err)
	require.Equal(t, want, end, "DLT end offset")
}
