package main

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

func sendToDLT(
	ctx context.Context,
	client *kgo.Client,
	cfg Config,
	record *kgo.Record,
	reason string,
) error {
	dltRecord := &kgo.Record{
		Topic: cfg.DLTTopic,
		Key:   record.Key,
		Value: record.Value,
		Headers: []kgo.RecordHeader{
			{Key: "error_reason", Value: []byte(reason)},
			{Key: "source_topic", Value: []byte(record.Topic)},
			{Key: "source_partition", Value: []byte(fmt.Sprint(record.Partition))},
			{Key: "source_offset", Value: []byte(fmt.Sprint(record.Offset))},
		},
	}

	return client.ProduceSync(ctx, dltRecord).FirstErr()
}
