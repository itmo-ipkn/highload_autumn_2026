package main

import "testing"

func TestCountInversions(t *testing.T) {
	for _, tc := range []struct {
		seq  []uint64
		want uint64
	}{
		{nil, 0},
		{[]uint64{1, 2, 3}, 0},
		{[]uint64{3, 1, 2}, 2},
		{[]uint64{3, 2, 1}, 3},
		{[]uint64{2, 2, 1}, 2},
	} {
		if got := countInversions(tc.seq); got != tc.want {
			t.Errorf("countInversions = %d, want %d", got, tc.want)
		}
	}
}
