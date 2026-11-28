package main

import (
	"fmt"
	"sort"
)

type userEvents struct {
	sequences map[int64]struct{}
	writes    []int64
}

type snapshot struct {
	users                    map[int64]*userEvents
	sink, dlt                map[string]struct{}
	duplicates               int64
	unknown, badSink, badDLT int
}

func (s *snapshot) add(user, seq int64) *userEvents {
	events := s.users[user]
	if events == nil {
		events = &userEvents{sequences: make(map[int64]struct{})}
		s.users[user] = events
	}
	events.sequences[seq] = struct{}{}
	return events
}

func sortedKeys[V any](values map[int64]V) []int64 {
	keys := make([]int64, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func inversions(writes, sequences []int64) (pairs int64, late int) {
	tree := make([]int, len(sequences)+1)
	for seen, seq := range writes {
		rank := sort.Search(len(sequences), func(i int) bool { return sequences[i] >= seq }) + 1
		lower := 0
		for i := rank; i > 0; i -= i & -i {
			lower += tree[i]
		}
		greater := seen - lower
		pairs += int64(greater)
		if greater > 0 {
			late++
		}
		for i := rank; i < len(tree); i += i & -i {
			tree[i]++
		}
	}
	return pairs, late
}

func (s *snapshot) report(before, after [3]partitionProgress, progressErr error) int {
	type gap struct{ user, first, last int64 }
	var gaps []gap
	var affectedUsers int
	var missing, pairs int64
	var late, overlap int
	for _, user := range sortedKeys(s.users) {
		events := s.users[user]
		sequences := sortedKeys(events.sequences)
		var previous int64
		firstGap := len(gaps)
		for _, seq := range sequences {
			if seq-previous > 1 {
				missing += seq - previous - 1
				gaps = append(gaps, gap{user, previous + 1, seq - 1})
			}
			previous = seq
		}
		if len(gaps) > firstGap {
			affectedUsers++
		}
		count, eventsLate := inversions(events.writes, sequences)
		pairs += count
		late += eventsLate
	}

	for id := range s.dlt {
		if _, exists := s.sink[id]; exists {
			overlap++
		}
	}

	var lag int64
	for _, partition := range after {
		lag += partition.end - partition.committed
	}

	complete := progressErr == nil && lag == 0 && before == after && len(s.users) > 0
	switch {
	case !complete:
		fmt.Println("Result: INCONCLUSIVE")
	case missing > 0 || overlap > 0 || s.badSink > 0 || s.badDLT > 0:
		fmt.Println("Result: FAILED (event continuity or DLT validation)")
	case s.unknown > 0:
		fmt.Println("Result: PARTIAL (some sink rows have no write order)")
	case pairs > 0:
		fmt.Println("Result: NO SEQUENCE GAPS; ORDER VIOLATIONS FOUND")
	default:
		fmt.Println("Result: NO SEQUENCE GAPS OR ORDER VIOLATIONS")
	}

	fmt.Printf("Missing seq in sink + DLT (1..max observed): %d across %d of %d users\n", missing, affectedUsers, len(s.users))
	fmt.Printf("Duplicate deliveries discarded: %d\nOrder violations (inverted pairs): %d\n", s.duplicates, pairs)
	fmt.Printf("Sink events: %d\nDLT events (unique event_id): %d\nLate sink events: %d\n", len(s.sink), len(s.dlt), late)
	fmt.Printf("Sink rows without write order: %d\nEvents present in both sink and DLT: %d\n", s.unknown, overlap)

	switch {
	case progressErr != nil:
		fmt.Printf("Kafka lag could not be checked: %v\n", progressErr)
	case lag > 0:
		fmt.Printf("Kafka lag: %d. Stop producers and rerun after all partitions reach zero lag.\n", lag)
	case before != after:
		fmt.Println("Kafka offsets moved during verification. Stop producers and retry on a drained group.")
	default:
		fmt.Println("Kafka lag: 0 (offsets stable during verification)")
	}

	for _, gap := range gaps {
		fmt.Printf("user_id=%d missing_seq=%d", gap.user, gap.first)
		if gap.first != gap.last {
			fmt.Printf("..%d", gap.last)
		}
		fmt.Println()
	}

	if s.badSink > 0 {
		fmt.Printf("Poison events incorrectly saved in sink: %d\n", s.badSink)
	}
	if s.badDLT > 0 {
		fmt.Printf("DLT records without the poison marker: %d\n", s.badDLT)
	}
	if len(s.users) == 0 {
		fmt.Println("No data to verify")
	}
	if !complete {
		return 2
	}
	if s.unknown > 0 {
		fmt.Println("Order check covers only rows written after the sink_order migration")
	}
	if missing > 0 || overlap > 0 || s.unknown > 0 || s.badSink > 0 || s.badDLT > 0 {
		return 1
	}
	return 0
}
