package verify

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"

	"highload/hw2/consumer/internal/event"
)

// Track distinct IDs only for slots with collisions; redelivery does not change ownership.
type sequenceOwners struct {
	first     map[sequence]uuid.UUID
	conflicts map[sequence]map[uuid.UUID]struct{}
}

func (s sequenceOwners) add(seq sequence, id uuid.UUID) {
	first, ok := s.first[seq]
	if !ok {
		s.first[seq] = id
	} else if id != first {
		if s.conflicts[seq] == nil {
			s.conflicts[seq] = map[uuid.UUID]struct{}{first: {}}
		}
		s.conflicts[seq][id] = struct{}{}
	}
}

func (s sequenceOwners) examples() []string {
	var examples []string
	for seq, owners := range s.conflicts {
		ids := make([]string, 0, len(owners))
		for id := range owners {
			ids = append(ids, id.String())
		}
		sort.Strings(ids)
		examples = append(examples, fmt.Sprintf("%s:%d (%s)", seq.user, seq.num, strings.Join(ids, ", ")))
	}
	sort.Strings(examples)
	return examples
}

func compare(source, dlt []event.Event, sink sinkSnapshot) Result {
	r := Result{DLTEvents: len(dlt), SinkEvents: int64(len(sink.events)), DuplicateDeliveries: sink.duplicates, OrderViolations: sink.orderViolations}
	owners := sequenceOwners{make(map[sequence]uuid.UUID), make(map[sequence]map[uuid.UUID]struct{})}
	delivered := make(map[sequence]bool)
	for id, seq := range sink.events {
		owners.add(seq, id)
		delivered[seq] = true
	}
	dltSeen := make(map[uuid.UUID]bool)
	poison := make(map[uuid.UUID]sequence)
	for _, e := range dlt {
		seq := sequence{e.UserID, e.Seq}
		owners.add(seq, e.EventID)
		if dltSeen[e.EventID] {
			r.DuplicateDLTEvents++
			continue
		}
		dltSeen[e.EventID] = true
		delivered[seq] = true
		if event.IsPoison(e.Payload) {
			poison[e.EventID] = seq
			if _, ok := sink.events[e.EventID]; ok {
				r.PoisonNotInDLT = append(r.PoisonNotInDLT, e.EventID.String()+" (also in sink)")
			}
		} else {
			r.NonPoisonInDLT = append(r.NonPoisonInDLT, e.EventID.String())
		}
	}
	r.UniqueDLTEvents, r.PoisonEvents = len(dltSeen), len(poison)

	maxSeq := make(map[string]uint64)
	type identity struct {
		seq sequence
		id  uuid.UUID
	}
	seen := make(map[identity]bool)
	for _, e := range source {
		seq := sequence{e.UserID, e.Seq}
		key := identity{seq, e.EventID}
		if seen[key] {
			continue
		}
		seen[key] = true
		owners.add(seq, e.EventID)
		maxSeq[e.UserID] = max(maxSeq[e.UserID], e.Seq)
		if event.IsPoison(e.Payload) {
			if poison[e.EventID] != seq {
				r.PoisonNotInDLT = append(r.PoisonNotInDLT, e.EventID.String())
			}
		} else if sink.events[e.EventID] != seq {
			r.MissingSinkEvents = append(r.MissingSinkEvents, e.EventID.String())
		}
	}
	for user, n := range maxSeq {
		for i := range n {
			if !delivered[sequence{user, i + 1}] {
				r.MissingSequences = append(r.MissingSequences, fmt.Sprintf("%s:%d", user, i+1))
			}
		}
	}
	r.SequenceConflicts = owners.examples()
	sort.Strings(r.MissingSequences)
	sort.Strings(r.MissingSinkEvents)
	sort.Strings(r.PoisonNotInDLT)
	sort.Strings(r.NonPoisonInDLT)
	return r
}

func (r Result) deliveryOK() bool {
	return len(r.MissingSequences) == 0 && len(r.SequenceConflicts) == 0 && len(r.MissingSinkEvents) == 0 &&
		len(r.PoisonNotInDLT) == 0 && len(r.NonPoisonInDLT) == 0 && r.ConsumerLag == 0
}

func (r Result) Passed() bool { return r.deliveryOK() && r.OrderViolations == 0 }

func (r Result) OrderOnly() bool { return r.deliveryOK() && r.OrderViolations > 0 }
