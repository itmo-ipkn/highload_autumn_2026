package main

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Read exactly the records up to the end offsets visible at the start of the read.
func readTopic(ctx context.Context, brokers []string, topic string, visit func(*kgo.Record) error) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	start, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return err
	}
	end, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return err
	}
	if len(end[topic]) == 0 {
		return fmt.Errorf("%s: no partitions", topic)
	}
	assign := map[int32]kgo.Offset{}
	pending := map[int32]int64{}
	for partition, last := range end[topic] {
		first, ok := start[topic][partition]
		if !ok || first.Err != nil || last.Err != nil || first.Offset > last.Offset {
			return fmt.Errorf("%s[%d]: invalid offset bounds: %v, %v", topic, partition, first.Err, last.Err)
		}
		if first.Offset < last.Offset {
			assign[partition] = kgo.NoResetOffset().At(first.Offset)
			pending[partition] = last.Offset
		}
	}
	if len(pending) == 0 {
		return nil
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: assign}))
	if err != nil {
		return err
	}
	defer reader.Close()
	for len(pending) > 0 {
		fetch := reader.PollFetches(ctx)
		if err := fetch.Err(); err != nil {
			return fmt.Errorf("read %s: %w", topic, err)
		}
		for _, record := range fetch.Records() {
			last, ok := pending[record.Partition]
			if !ok {
				continue
			}
			if record.Offset < last {
				if err := visit(record); err != nil {
					return fmt.Errorf("%s[%d] offset %d: %w", topic, record.Partition, record.Offset, err)
				}
			}
			if record.Offset+1 >= last {
				delete(pending, record.Partition)
				reader.PauseFetchPartitions(map[string][]int32{topic: {record.Partition}})
			}
		}
	}
	return nil
}
