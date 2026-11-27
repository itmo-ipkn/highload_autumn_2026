package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Steplet/HL-lab-2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type report struct {
	processed  int
	deadLetter int
	duplicates int64
	gaps       uint64
	inversions uint64
	users      int
}

func countInversions(seq []uint64) uint64 {
	if len(seq) < 2 {
		return 0
	}
	var count uint64
	for width := 1; width < len(seq); width *= 2 {
		for left := 0; left < len(seq); left += 2 * width {
			mid := min(left+width, len(seq))
			right := min(left+2*width, len(seq))
			merged := make([]uint64, 0, right-left)
			i, j := left, mid
			for i < mid && j < right {
				if seq[i] <= seq[j] {
					merged = append(merged, seq[i])
					i++
				} else {
					merged = append(merged, seq[j])
					count += uint64(mid - i)
					j++
				}
			}
			merged = append(merged, seq[i:mid]...)
			merged = append(merged, seq[j:right]...)
			copy(seq[left:right], merged)
		}
	}
	return count
}

func readDLT(ctx context.Context, brokers string) ([]lab2.Event, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		return nil, err
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	starts, err := admin.ListStartOffsets(ctx, "events.dlt")
	if err != nil {
		return nil, err
	}
	ends, err := admin.ListEndOffsets(ctx, "events.dlt")
	if err != nil {
		return nil, err
	}
	if err := starts.Error(); err != nil {
		return nil, err
	}
	if err := ends.Error(); err != nil {
		return nil, err
	}
	positions := make(map[int32]int64)
	limits := make(map[int32]int64)
	partitions := make(map[int32]kgo.Offset)
	for partition, start := range starts["events.dlt"] {
		end, ok := ends.Lookup("events.dlt", partition)
		if !ok {
			return nil, fmt.Errorf("missing DLT end offset for partition %d", partition)
		}
		positions[partition] = start.Offset
		limits[partition] = end.Offset
		if start.Offset < end.Offset {
			partitions[partition] = kgo.NewOffset().At(start.Offset)
		}
	}
	if len(starts["events.dlt"]) == 0 {
		return nil, errors.New("events.dlt has no partitions")
	}
	if len(partitions) == 0 {
		return nil, nil
	}
	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{"events.dlt": partitions})
	var events []lab2.Event
	for {
		complete := true
		for partition, limit := range limits {
			if positions[partition] < limit {
				complete = false
				break
			}
		}
		if complete {
			return events, nil
		}
		fetches := client.PollRecords(ctx, 500)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, errs[0].Err
		}
		for _, record := range fetches.Records() {
			if record.Offset >= limits[record.Partition] {
				continue
			}
			positions[record.Partition] = record.Offset + 1
			var event lab2.Event
			if err := json.Unmarshal(record.Value, &event); err != nil {
				return nil, fmt.Errorf("decode DLT partition %d offset %d: %w", record.Partition, record.Offset, err)
			}
			events = append(events, event)
		}
	}
}

func verify(ctx context.Context, dsn, brokers string) (report, error) {
	var result report
	// Read DLT first so later sink rows cannot hide poison events in the snapshot.
	dlt, err := readDLT(ctx, brokers)
	if err != nil {
		return result, err
	}
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return result, err
	}
	defer db.Close(context.Background())
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT user_id, seq FROM processed_events ORDER BY sink_order`)
	if err != nil {
		return result, err
	}
	byUser := make(map[uuid.UUID][]uint64)
	seen := make(map[uuid.UUID]map[uint64]struct{})
	for rows.Next() {
		var user uuid.UUID
		var seq uint64
		if err := rows.Scan(&user, &seq); err != nil {
			rows.Close()
			return result, err
		}
		byUser[user] = append(byUser[user], seq)
		if seen[user] == nil {
			seen[user] = make(map[uint64]struct{})
		}
		seen[user][seq] = struct{}{}
		result.processed++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, err
	}
	rows.Close()
	if err := tx.QueryRow(ctx, `SELECT count FROM duplicate_attempts WHERE id = 1`).Scan(&result.duplicates); err != nil {
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		return result, err
	}
	deadIDs := make(map[uuid.UUID]struct{})
	for _, event := range dlt {
		if _, ok := deadIDs[event.EventID]; ok {
			continue
		}
		deadIDs[event.EventID] = struct{}{}
		if seen[event.UserID] == nil {
			seen[event.UserID] = make(map[uint64]struct{})
		}
		seen[event.UserID][event.Seq] = struct{}{}
		result.deadLetter++
	}
	for user, seqs := range seen {
		result.users++
		var maxSeq uint64
		for seq := range seqs {
			maxSeq = max(maxSeq, seq)
		}
		if maxSeq > uint64(len(seqs)) {
			result.gaps += maxSeq - uint64(len(seqs))
		}
		ordered := byUser[user]
		result.inversions += countInversions(ordered)
	}
	return result, nil
}

func main() {
	fmt.Printf("Steplet %s\n", time.Now().Format(time.RFC3339))
	dsn, brokers := os.Getenv("PG_DSN"), os.Getenv("KAFKA_BROKERS")
	if dsn == "" || brokers == "" {
		fmt.Fprintln(os.Stderr, "PG_DSN and KAFKA_BROKERS are required")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	r, err := verify(ctx, dsn, brokers)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("users: %d\nprocessed: %d\nDLT: %d\nduplicate attempts: %d\nseq gaps: %d\norder inversions: %d\n", r.users, r.processed, r.deadLetter, r.duplicates, r.gaps, r.inversions)
	fmt.Println("seq gaps are provisional while consumers are still processing")
}
