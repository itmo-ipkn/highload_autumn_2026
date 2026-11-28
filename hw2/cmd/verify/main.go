package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafka-events/internal/config"
	"kafka-events/internal/model"
	"kafka-events/internal/storage"
)

func main() {
	cfg, err := config.LoadVerifyConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	fmt.Printf("GITHUB_LOGIN=%s CURRENT_TIME=%s\n", cfg.User, time.Now().Format(time.RFC3339))

	if err := run(&cfg); err != nil {
		fmt.Fprintf(os.Stderr, "verify failed: %v\n", err)
		os.Exit(2)
	}
}

func run(cfg *config.VerifyConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	db, err := storage.New(ctx, cfg.PgURL)
	if err != nil {
		return fmt.Errorf("PostgreSQL: %w", err)
	}
	defer db.Close()

	adminClient, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID("verify-admin"))
	if err != nil {
		return fmt.Errorf("failed to create kafka admin client: %w", err)
	}
	defer adminClient.Close()

	admin := kadm.NewClient(adminClient)
	sourceEvents, err := readTopic(ctx, admin, cfg.Brokers, model.TopicEvents)
	if err != nil {
		return fmt.Errorf("read events topic: %w", err)
	}

	dltEvents, err := readTopic(ctx, admin, cfg.Brokers, model.TopicDLT)
	if err != nil {
		return fmt.Errorf("read DLT topic: %w", err)
	}

	sinkEvents, err := db.SinkEvents(ctx)
	if err != nil {
		return fmt.Errorf("read sink: %w", err)
	}

	duplicates, err := db.DuplicateCount(ctx)
	if err != nil {
		return fmt.Errorf("read duplicate counter: %w", err)
	}

	sourceByID := make(map[string]model.Event, len(sourceEvents))
	expectedMaxSeq := make(map[string]int64)
	for _, event := range sourceEvents {
		sourceByID[event.EventID] = event
		if event.Seq > expectedMaxSeq[event.UserID] {
			expectedMaxSeq[event.UserID] = event.Seq
		}
	}

	sinkIDs := make(map[string]struct{}, len(sinkEvents))
	presentSeq := make(map[string]map[int64]struct{})
	for _, event := range sinkEvents {
		sinkIDs[event.EventID] = struct{}{}
		addSequence(presentSeq, event.UserID, event.Seq)
	}

	dltIDs := make(map[string]struct{}, len(dltEvents))
	for _, event := range dltEvents {
		dltIDs[event.EventID] = struct{}{}
		if event.UserID != "" && event.Seq > 0 {
			addSequence(presentSeq, event.UserID, event.Seq)
		}
	}

	gaps := countSequenceGaps(presentSeq, expectedMaxSeq)
	unhandled, poisonMissing, poisonInSink, nonPoisonInDLT := checkOutcomes(sourceByID, sinkIDs, dltIDs)
	orderInversions := countOrderInversions(sinkEvents)
	poisonSource := countSourcePoison(sourceByID)
	poisonRate := 0.0
	if len(sourceByID) > 0 {
		poisonRate = float64(poisonSource) * 100 / float64(len(sourceByID))
	}

	fmt.Println("=== VERIFY ===")
	fmt.Printf("events_source_records=%d\n", len(sourceEvents))
	fmt.Printf("events_source_unique=%d\n", len(sourceByID))
	fmt.Printf("sink_events=%d\n", len(sinkEvents))
	fmt.Printf("dlt_records=%d\n", len(dltEvents))
	fmt.Printf("source_poison_events=%d\n", poisonSource)
	fmt.Printf("source_poison_rate_percent=%.2f\n", poisonRate)
	fmt.Printf("users=%d\n", len(expectedMaxSeq))
	fmt.Printf("sequence_gaps=%d\n", gaps)
	fmt.Printf("unhandled_source_events=%d\n", unhandled)
	fmt.Printf("duplicates_dropped=%d\n", duplicates)
	fmt.Printf("out_of_order_pairs=%d\n", orderInversions)
	fmt.Printf("poison_missing_from_dlt=%d\n", poisonMissing)
	fmt.Printf("poison_events_in_sink=%d\n", poisonInSink)
	fmt.Printf("non_poison_events_in_dlt=%d\n", nonPoisonInDLT)

	if gaps != 0 || unhandled != 0 || poisonMissing != 0 || poisonInSink != 0 || nonPoisonInDLT != 0 {
		return fmt.Errorf("verification failed: gaps=%d unhandled=%d poison_missing=%d poison_in_sink=%d non_poison_in_dlt=%d",
			gaps, unhandled, poisonMissing, poisonInSink, nonPoisonInDLT)
	}
	return nil
}

func addSequence(all map[string]map[int64]struct{}, userID string, seq int64) {
	if _, exists := all[userID]; !exists {
		all[userID] = make(map[int64]struct{})
	}
	all[userID][seq] = struct{}{}
}

func countSequenceGaps(present map[string]map[int64]struct{}, expectedMax map[string]int64) int64 {
	var total int64
	for userID, maxSeq := range expectedMax {
		for seq := int64(1); seq <= maxSeq; seq++ {
			if _, ok := present[userID][seq]; ok {
				continue
			}
			total++
		}
	}
	return total
}

func checkOutcomes(source map[string]model.Event, sinkIDs, dltIDs map[string]struct{}) (unhandled, poisonMissing, poisonInSink, nonPoisonInDLT int64) {
	for eventID, event := range source {
		_, inSink := sinkIDs[eventID]
		_, inDLT := dltIDs[eventID]

		if !inSink && !inDLT {
			unhandled++
		}
		if model.IsPoison(event) {
			if !inDLT {
				poisonMissing++
			}
			if inSink {
				poisonInSink++
			}
		} else if inDLT {
			nonPoisonInDLT++
		}
	}
	return
}

func countSourcePoison(source map[string]model.Event) int64 {
	var count int64
	for _, event := range source {
		if model.IsPoison(event) {
			count++
		}
	}
	return count
}

func countOrderInversions(events []model.SinkEvent) int64 {
	byUser := make(map[string][]model.SinkEvent)
	for _, event := range events {
		byUser[event.UserID] = append(byUser[event.UserID], event)
	}

	var total int64
	for _, userEvents := range byUser {
		sort.Slice(userEvents, func(i, j int) bool {
			return userEvents[i].WriteOrder < userEvents[j].WriteOrder
		})
		for i := 0; i < len(userEvents); i++ {
			for j := i + 1; j < len(userEvents); j++ {
				if userEvents[i].Seq > userEvents[j].Seq {
					total++
				}
			}
		}
	}
	return total
}

func readTopic(ctx context.Context, admin *kadm.Client, brokers []string, topic string) ([]model.Event, error) {
	endOffsets, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	if err := endOffsets.Error(); err != nil {
		return nil, err
	}

	starts := make(map[int32]kgo.Offset)
	ends := make(map[int32]int64)
	finished := make(map[int32]bool)
	for partition, end := range endOffsets[topic] {
		starts[partition] = kgo.NewOffset().AtStart()
		ends[partition] = end.Offset
		finished[partition] = end.Offset == 0
	}
	if len(starts) == 0 {
		return nil, fmt.Errorf("topic %s has no partitions", topic)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("verify-"+topic),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: starts}),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	var events []model.Event
	for !allFinished(finished) {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("read %s timed out: %w", topic, err)
		}
		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		fetches := client.PollRecords(pollCtx, 1000)
		cancel()
		if fetches.IsClientClosed() {
			return nil, fmt.Errorf("reader for %s was closed unexpectedly", topic)
		}

		var fetchErr error
		fetches.EachError(func(fetchTopic string, partition int32, err error) {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && fetchErr == nil {
				fetchErr = fmt.Errorf("fetch %s[%d]: %w", fetchTopic, partition, err)
			}
		})
		if fetchErr != nil {
			return nil, fetchErr
		}

		for _, record := range fetches.Records() {
			if record.Offset >= ends[record.Partition] {
				finished[record.Partition] = true
				continue
			}
			var event model.Event
			if err := json.Unmarshal(record.Value, &event); err != nil {
				return nil, fmt.Errorf("decode %s[%d]@%d: %w", topic, record.Partition, record.Offset, err)
			}
			events = append(events, event)
			if record.Offset+1 >= ends[record.Partition] {
				finished[record.Partition] = true
			}
		}
	}
	return events, nil
}

func allFinished(finished map[int32]bool) bool {
	for _, done := range finished {
		if !done {
			return false
		}
	}
	return true
}
