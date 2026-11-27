package verify

import (
	"errors"
	"testing"
)

func TestEvalLag(t *testing.T) {
	boom := errors.New("NOT_LEADER_FOR_PARTITION")
	cases := []struct {
		name  string
		state string
		parts []PartitionLag
		want  LagState
	}{
		{"all zero stable", "Stable", []PartitionLag{{Lag: 0}, {Lag: 0}, {Lag: 0}}, LagState{Total: 0, CaughtUp: true}},
		{"lag left", "Stable", []PartitionLag{{Lag: 0}, {Lag: 5}, {Lag: 0}}, LagState{Total: 5}},
		{"rebalancing", "PreparingRebalance", []PartitionLag{{Lag: 0}, {Lag: 0}}, LagState{Total: 0}},
		{"partition error", "Stable", []PartitionLag{{Lag: 0}, {Lag: -1, Err: boom}, {Lag: 0}}, LagState{Total: -1}},
		{"negative lag", "Stable", []PartitionLag{{Lag: 0}, {Lag: -1}}, LagState{Total: -1}},
		{"no partitions", "Stable", nil, LagState{Total: -1}},
	}
	for _, c := range cases {
		if got := EvalLag(c.state, c.parts); got != c.want {
			t.Errorf("%s: %+v, want %+v", c.name, got, c.want)
		}
	}
}

func TestSettled(t *testing.T) {
	ok := LagState{CaughtUp: true}
	no := LagState{Total: 3}
	if Settled([]LagState{ok}) {
		t.Fatal("one measurement is not enough")
	}
	if Settled([]LagState{ok, no, ok}) {
		t.Fatal("last two must both be caught up")
	}
	if !Settled([]LagState{no, ok, ok}) {
		t.Fatal("two caught up in a row")
	}
}
