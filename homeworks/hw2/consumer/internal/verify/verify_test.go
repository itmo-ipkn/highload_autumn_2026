package verify

import "testing"

func TestSequenceSetTracksSequencesPerUser(t *testing.T) {
	sequences := make(sequenceSet)
	sequences.add("user-a", 1)
	sequences.add("user-a", 3)
	sequences.add("user-b", 1)

	if len(sequences["user-a"]) != 2 {
		t.Fatalf("user-a sequences = %#v, want 2 distinct values", sequences["user-a"])
	}
	if _, exists := sequences["user-a"][3]; !exists {
		t.Fatal("sequence 3 for user-a was not recorded")
	}
	if _, exists := sequences["user-b"][3]; exists {
		t.Fatal("sequence set leaked between users")
	}

	sequences.add("user-a", 3)
	if len(sequences["user-a"]) != 2 {
		t.Fatalf("duplicate sequence changed set size: %#v", sequences["user-a"])
	}
}

func TestResultPassed(t *testing.T) {
	tests := []struct {
		name   string
		result Result
		want   bool
	}{
		{name: "clean result", result: Result{}, want: true},
		{name: "missing sequences", result: Result{MissingSequences: []string{"user-a:2"}}},
		{name: "poison missing from DLT", result: Result{PoisonNotInDLT: []string{"event-id"}}},
		{name: "non-poison in DLT", result: Result{NonPoisonInDLT: []string{"event-id"}}},
		{name: "out of order", result: Result{OrderViolations: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.result.Passed(); got != tt.want {
				t.Fatalf("Passed() = %v, want %v", got, tt.want)
			}
		})
	}
}
