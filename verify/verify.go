package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Event struct {
	UserID string `json:"user_id"`
	Seq    int    `json:"seq"`
}

func main() {
	login := "BruhSlave"
	fmt.Printf("%s %s\n", login, time.Now().Format(time.RFC3339))

	ctx := context.Background()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://kafka:kafka_pass@localhost:5432/events_sink"
	}

	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close(ctx)

	sinkEvents, err := loadSink(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	dltEvents, err := loadDLT()
	if err != nil {
		log.Fatal(err)
	}

	printGaps(sinkEvents, dltEvents)
	printDuplicates(ctx, db)
	printOrderViolations(ctx, db)
}

func loadSink(ctx context.Context, db *pgx.Conn) (map[string]map[int]bool, error) {
	result := make(map[string]map[int]bool)

	rows, err := db.Query(ctx, `SELECT user_id, seq FROM processed_events`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var user string
		var seq int

		if err := rows.Scan(&user, &seq); err != nil {
			return nil, err
		}

		addSeq(result, user, seq)
	}

	return result, rows.Err()
}

func loadDLT() (map[string]map[int]bool, error) {
	result := make(map[string]map[int]bool)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(getEnv("BROKERS", "localhost:9092")),
		kgo.ConsumerGroup("verify-dlt"),
		kgo.ConsumeTopics("events.dlt"),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		fetches := client.PollRecords(ctx, 1000)
		cancel()

		if len(fetches.Records()) == 0 {
			break
		}

		var fetchErr error
		fetches.EachError(func(topic string, partition int32, err error) {
			fetchErr = err
		})
		if fetchErr != nil {
			return nil, fetchErr
		}

		for _, record := range fetches.Records() {
			var event Event
			if err := json.Unmarshal(record.Value, &event); err != nil {
				continue
			}
			addSeq(result, event.UserID, event.Seq)
		}
	}

	return result, nil
}

func addSeq(events map[string]map[int]bool, user string, seq int) {
	if events[user] == nil {
		events[user] = make(map[int]bool)
	}
	events[user][seq] = true
}

func printGaps(sink, dlt map[string]map[int]bool) int {
	all := make(map[string]map[int]bool)

	for user, seqs := range sink {
		for seq := range seqs {
			addSeq(all, user, seq)
		}
	}
	for user, seqs := range dlt {
		for seq := range seqs {
			addSeq(all, user, seq)
		}
	}

	totalGaps := 0
	totalExpected := 0
	totalUnique := 0

	for _, seqs := range all {
		maxSeq := 0
		for seq := range seqs {
			if seq > maxSeq {
				maxSeq = seq
			}
		}

		totalExpected += maxSeq

		for seq := 1; seq <= maxSeq; seq++ {
			if seqs[seq] {
				totalUnique++
			} else {
				totalGaps++
			}
		}
	}

	fmt.Println("\nVERIFICATION SUMMARY")
	fmt.Printf("%-38s %s\n", "CHECK", "RESULT")
	fmt.Println("-----------------------------------------------")
	fmt.Printf("%-38s %d\n", "Total users", len(all))
	fmt.Printf("%-38s %d\n", "Missing sequences", totalGaps)
	fmt.Printf("%-38s %d\n", "Expected sequences", totalExpected)
	fmt.Printf("%-38s %d\n", "Unique sequences in sink + DLT", totalUnique)

	if totalGaps == 0 {
		fmt.Println("\n                 NO DATA LOSS")
	} else {
		fmt.Printf("\n                 WARNING: %d MISSING SEQUENCES\n", totalGaps)
	}

	return totalGaps
}

func printDuplicates(ctx context.Context, db *pgx.Conn) {
	var total int64

	err := db.QueryRow(ctx, `
		SELECT COALESCE(SUM(duplicates_count), 0)
		FROM consumer_stats
	`).Scan(&total)

	if err != nil {
		log.Printf("failed to read duplicate stats: %v", err)
		return
	}

	fmt.Println("\n=== DEDUPLICATION ===")
	fmt.Printf("Discarded duplicate deliveries: %d\n", total)
}

func printOrderViolations(ctx context.Context, db *pgx.Conn) {
	var count int64

	err := db.QueryRow(ctx, `
		WITH ordered AS (
			SELECT
				user_id,
				seq,
				MAX(seq) OVER (
					PARTITION BY user_id
					ORDER BY processed_at, event_id
					ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
				) AS previous_max_seq
			FROM processed_events
		)
		SELECT COUNT(*)
		FROM ordered
		WHERE seq < previous_max_seq
	`).Scan(&count)

	if err != nil {
		log.Printf("failed to check ordering: %v", err)
		return
	}

	fmt.Println("\n=== ORDER CHECK ===")
	fmt.Printf("Out-of-order events: %d\n", count)
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
