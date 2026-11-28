package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"slices"
	"time"
	_ "time/tzdata"

	"github.com/twmb/franz-go/pkg/kgo"
)

type event struct {
	ID         string    `json:"event_id"`
	User       string    `json:"user_id"`
	Seq        int64     `json:"seq"`
	ProducedAt time.Time `json:"produced_at"`
	Payload    struct {
		Poison bool `json:"poison"`
	} `json:"payload"`
}

func verify(args []string, brokers []string, db *sql.DB) error {
	fmt.Printf("%s %s\n", env("GITHUB_LOGIN", "SAPIPA"), time.Now().Format(time.RFC3339))
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	sinceText := flags.String("since", "", "include events generated from this RFC3339 timestamp")
	timeout := flags.Duration("timeout", 2*time.Minute, "deadline for reading the snapshot")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	var since time.Time
	if *sinceText != "" {
		parsed, err := time.Parse(time.RFC3339, *sinceText)
		if err != nil {
			return fmt.Errorf("--since: %w", err)
		}
		since = parsed
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	expected := map[string]event{}
	if err := readTopic(ctx, brokers, events, func(record *kgo.Record) error {
		var value event
		if err := json.Unmarshal(record.Value, &value); err != nil {
			return err
		}
		if value.ID == "" || value.User == "" || value.Seq < 1 || value.ProducedAt.IsZero() {
			return fmt.Errorf("incomplete event %q", value.ID)
		}
		if !value.ProducedAt.Before(since) {
			expected[value.ID] = value
		}
		return nil
	}); err != nil {
		return err
	}
	if len(expected) == 0 {
		return fmt.Errorf("no events in the requested window")
	}

	seen := map[string]bool{}
	sequences := map[string][]int64{}
	var saved, dead, misplaced, outOfOrder, duplicateDLT int
	if err := readTopic(ctx, brokers, dlt, func(record *kgo.Record) error {
		var value event
		if err := json.Unmarshal(record.Value, &value); err != nil {
			return err
		}
		original, ok := expected[value.ID]
		if !ok {
			return nil
		}
		if !original.Payload.Poison || original.User != value.User || original.Seq != value.Seq {
			misplaced++
		} else if seen[value.ID] {
			duplicateDLT++
		} else {
			dead++
			seen[value.ID] = true
			sequences[value.User] = append(sequences[value.User], value.Seq)
		}
		return nil
	}); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var duplicates int64
	if err := tx.QueryRowContext(ctx, "SELECT duplicates FROM counters WHERE id = 1").Scan(&duplicates); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, "SELECT event_id, user_id, seq FROM processed_events ORDER BY id")
	if err != nil {
		return err
	}
	defer rows.Close()
	maxSeq := map[string]int64{}
	for rows.Next() {
		var id, user string
		var seq int64
		if err := rows.Scan(&id, &user, &seq); err != nil {
			return err
		}
		original, ok := expected[id]
		if !ok {
			continue
		}
		if original.Payload.Poison || original.User != user || original.Seq != seq {
			misplaced++
			continue
		}
		saved++
		seen[id] = true
		sequences[user] = append(sequences[user], seq)
		if seq < maxSeq[user] {
			outOfOrder++
		}
		maxSeq[user] = max(seq, maxSeq[user])
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	lastSeq := map[string]int64{}
	for _, value := range expected {
		lastSeq[value.User] = max(lastSeq[value.User], value.Seq)
	}
	users := make([]string, 0, len(lastSeq))
	for user := range lastSeq {
		users = append(users, user)
	}
	slices.Sort(users)
	var missingSeq int64
	for _, user := range users {
		seqs := sequences[user]
		slices.Sort(seqs)
		last := int64(0)
		for _, seq := range seqs {
			if seq > last+1 {
				fmt.Printf("missing_seq user_id=%s range=%d..%d\n", user, last+1, seq-1)
				missingSeq += seq - last - 1
			}
			last = max(last, seq)
		}
		if last < lastSeq[user] {
			fmt.Printf("missing_seq user_id=%s range=%d..%d\n", user, last+1, lastSeq[user])
			missingSeq += lastSeq[user] - last
		}
	}
	missing := len(expected) - len(seen)
	fmt.Printf("users=%d events=%d sink=%d dlt=%d missing_events=%d missing_seq=%d misplaced=%d\n",
		len(users), len(expected), saved, dead, missing, missingSeq, misplaced)
	fmt.Printf("duplicates_discarded_total=%d dlt_repeats=%d out_of_order=%d\n", duplicates, duplicateDLT, outOfOrder)
	if missing > 0 || missingSeq > 0 || misplaced > 0 {
		return fmt.Errorf("verification failed: missing or misplaced events")
	}
	return nil
}
