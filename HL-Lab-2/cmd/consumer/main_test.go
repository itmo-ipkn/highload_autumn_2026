package main

import (
	"strconv"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDeadLetterRecord(t *testing.T) {
	source := &kgo.Record{Topic: "events", Partition: 2, Offset: 42, Key: []byte("user"), Value: []byte(`{"poison":true}`)}
	dlt := deadLetterRecord(source, "poison event")
	if dlt.Topic != "events.dlt" || string(dlt.Key) != string(source.Key) || string(dlt.Value) != string(source.Value) {
		t.Fatalf("wrong DLT record: %+v", dlt)
	}
	headers := make(map[string]string)
	for _, header := range dlt.Headers {
		headers[header.Key] = string(header.Value)
	}
	for key, want := range map[string]string{
		"error_reason":     "poison event",
		"source_topic":     "events",
		"source_partition": strconv.Itoa(int(source.Partition)),
		"source_offset":    strconv.FormatInt(source.Offset, 10),
	} {
		if headers[key] != want {
			t.Fatalf("header %s = %q, want %q", key, headers[key], want)
		}
	}
}
