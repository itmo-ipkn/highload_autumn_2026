package verify

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type partitionStub struct {
	sarama.PartitionConsumer
	messages chan *sarama.ConsumerMessage
	errs     chan *sarama.ConsumerError
	closed   bool
}

func (p *partitionStub) Messages() <-chan *sarama.ConsumerMessage { return p.messages }
func (p *partitionStub) Errors() <-chan *sarama.ConsumerError     { return p.errs }
func (p *partitionStub) Close() error                             { p.closed = true; return nil }

type consumerStub struct {
	sarama.Consumer
	open func(string, int32, int64) (sarama.PartitionConsumer, error)
}

func (c consumerStub) ConsumePartition(topic string, partition int32, start int64) (sarama.PartitionConsumer, error) {
	return c.open(topic, partition, start)
}

func TestPartitionSnapshot(t *testing.T) {
	for _, topic := range []string{"events", "events.dlt"} {
		t.Run(topic+"/empty", func(t *testing.T) {
			// No consumer should be opened for [9,9).
			got, err := readPartition(t.Context(), nil, topic, 1, 9, 9)
			if len(got) != 0 || err != nil {
				t.Fatalf("empty snapshot = %v, %v", got, err)
			}
		})
		for _, tc := range []struct {
			name    string
			offsets []int64
			want    int
		}{
			{"no later publication", []int64{9}, 1},
			{"later publication excluded", []int64{8, 9, 10}, 2},
			{"first available beyond end", []int64{11}, 0},
		} {
			t.Run(topic+"/"+tc.name, func(t *testing.T) {
				p := &partitionStub{messages: make(chan *sarama.ConsumerMessage, len(tc.offsets))}
				start := min(tc.offsets[0], 9)
				for _, offset := range tc.offsets {
					data, err := json.Marshal(testEvent("user", uint64(offset), `"value"`))
					if err != nil {
						t.Fatal(err)
					}
					p.messages <- &sarama.ConsumerMessage{Offset: offset, Value: data}
				}
				c := consumerStub{open: func(name string, part int32, from int64) (sarama.PartitionConsumer, error) {
					if name != topic || part != 1 || from != start {
						t.Fatalf("unexpected open: %s[%d]@%d", name, part, from)
					}
					return p, nil
				}}
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				got, err := readPartition(ctx, c, topic, 1, start, 10)
				if err != nil || !p.closed || len(got) != tc.want {
					t.Fatalf("snapshot = %v, %v, closed=%v", got, err, p.closed)
				}
				for i, e := range got {
					if e.Seq != uint64(tc.offsets[i]) {
						t.Fatalf("event %d: got seq %d", i, e.Seq)
					}
				}
			})
		}
	}
}

func TestPartitionSnapshotIncomplete(t *testing.T) {
	for _, name := range []string{"closed", "cancelled", "broker error", "invalid event"} {
		t.Run(name, func(t *testing.T) {
			p := &partitionStub{messages: make(chan *sarama.ConsumerMessage, 1), errs: make(chan *sarama.ConsumerError, 1)}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			switch name {
			case "closed":
				close(p.messages)
			case "cancelled":
				cancel()
			case "broker error":
				p.errs <- &sarama.ConsumerError{Err: sarama.ErrNotLeaderForPartition}
			case "invalid event":
				p.messages <- &sarama.ConsumerMessage{Value: []byte(`{}`)}
			}
			c := consumerStub{open: func(string, int32, int64) (sarama.PartitionConsumer, error) { return p, nil }}
			_, err := readPartition(ctx, c, "events", 1, 0, 10)
			if err == nil || errors.Is(err, context.DeadlineExceeded) || !p.closed {
				t.Fatalf("err = %v, closed=%v", err, p.closed)
			}
		})
	}
}
