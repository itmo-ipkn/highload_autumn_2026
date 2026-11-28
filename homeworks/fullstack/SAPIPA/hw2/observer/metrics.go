package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func serveMetrics(brokers []string, db *sql.DB) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequestRetries(1))
	if err != nil {
		return err
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
		defer cancel()
		writeSinkMetrics(ctx, w, db)
		for _, broker := range brokers {
			fmt.Fprintf(w, "hw4_broker_up{broker=%q} %d\n", broker, brokerUp(ctx, broker))
		}
		writeKafkaMetrics(ctx, w, admin)
	})
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Print("serving metrics on :8080")
	return server.ListenAndServe()
}

func brokerUp(ctx context.Context, broker string) int {
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	client, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.DialTimeout(500*time.Millisecond), kgo.RequestRetries(0))
	if err != nil {
		return 0
	}
	defer client.Close()
	if client.Ping(ctx) != nil {
		return 0
	}
	return 1
}

func writeSinkMetrics(ctx context.Context, w io.Writer, db *sql.DB) {
	rows, err := db.QueryContext(ctx, "SELECT consumer_id, count(*) FROM processed_events GROUP BY consumer_id")
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var consumer, count int64
		if rows.Scan(&consumer, &count) == nil {
			fmt.Fprintf(w, "hw4_sink_events_total{consumer=%q} %d\n", fmt.Sprint(consumer), count)
		}
	}
	var duplicates int64
	if db.QueryRowContext(ctx, "SELECT duplicates FROM counters WHERE id = 1").Scan(&duplicates) == nil {
		fmt.Fprintf(w, "hw4_sink_duplicates_total %d\n", duplicates)
	}
}

func writeKafkaMetrics(ctx context.Context, w io.Writer, admin *kadm.Client) {
	if topics, err := admin.ListTopics(ctx, events); err == nil {
		for partition, entry := range topics[events].Partitions {
			if entry.Err == nil {
				fmt.Fprintf(w, "hw4_partition_isr{partition=%q} %d\n", fmt.Sprint(partition), len(entry.ISR))
				fmt.Fprintf(w, "hw4_partition_leader{partition=%q} %d\n", fmt.Sprint(partition), entry.Leader)
			}
		}
	}
	if offsets, err := admin.ListEndOffsets(ctx, events, dlt); err == nil {
		for topic, partitions := range offsets {
			for partition, offset := range partitions {
				if offset.Err == nil {
					fmt.Fprintf(w, "hw4_end_offset{topic=%q,partition=%q} %d\n", topic, fmt.Sprint(partition), offset.Offset)
				}
			}
		}
	}
	groups, err := admin.Lag(ctx, group)
	if detail, ok := groups[group]; err == nil && ok && detail.Error() == nil {
		fmt.Fprintf(w, "hw4_group_members %d\n", len(detail.Members))
		for partition, member := range detail.Lag[events] {
			if member.Err == nil {
				fmt.Fprintf(w, "hw4_group_lag{partition=%q} %d\n", fmt.Sprint(partition), member.Lag)
			}
		}
	}
}
