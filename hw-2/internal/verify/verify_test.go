package verify

import (
	"strings"
	"testing"
	"time"
)

func rows(user string, seqs ...int64) []Row {
	out := make([]Row, len(seqs))
	for i, s := range seqs {
		out[i] = Row{UserID: user, Seq: s}
	}
	return out
}

func TestAnalyzeClean(t *testing.T) {
	sink := append(rows("a", 1, 2, 4, 5), rows("b", 1, 2)...)
	r := Analyze(sink, []Key{{"a", 3}, {"b", 3}})
	if r.Users != 2 || r.SinkEvents != 6 || r.DLTUnique != 2 {
		t.Fatalf("%+v", r)
	}
	if r.Lost != 0 || r.MaxSeq != 8 || r.UniqueSeq != 8 || !r.OK() {
		t.Fatalf("lost %d max %d unique %d", r.Lost, r.MaxSeq, r.UniqueSeq)
	}
	if r.OrderViolations != 0 {
		t.Fatalf("violations %d", r.OrderViolations)
	}
}

func TestAnalyzeLost(t *testing.T) {
	// у a нет 3 и 4, у b нет 1; seq из dlt не потерян
	sink := append(rows("a", 1, 2, 5, 6), rows("b", 2)...)
	r := Analyze(sink, []Key{{"a", 6}, {"a", 6}})
	if r.Lost != 3 || r.UsersWithLoss != 2 || r.OK() {
		t.Fatalf("lost %d users %d", r.Lost, r.UsersWithLoss)
	}
	want := []Key{{"a", 3}, {"a", 4}, {"b", 1}}
	for i, k := range want {
		if r.LostExamples[i] != k {
			t.Fatalf("examples %v", r.LostExamples)
		}
	}
	if r.DLTRecords != 2 || r.DLTUnique != 1 || r.InBoth != 1 {
		t.Fatalf("dlt %d/%d inBoth %d", r.DLTRecords, r.DLTUnique, r.InBoth)
	}
}

func TestAnalyzeOrderViolations(t *testing.T) {
	// a: 1 3 2 5 4 -> 2 записан после 3, 4 после 5
	// b: 3 1 2 -> 1 и 2 записаны после 3
	sink := []Row{
		{UserID: "a", Seq: 1}, {UserID: "b", Seq: 3}, {UserID: "a", Seq: 3}, {UserID: "b", Seq: 1},
		{UserID: "a", Seq: 2}, {UserID: "a", Seq: 5}, {UserID: "b", Seq: 2}, {UserID: "a", Seq: 4},
	}
	r := Analyze(sink, nil)
	if r.OrderViolations != 4 || r.UsersWithViolations != 2 {
		t.Fatalf("violations %d users %d", r.OrderViolations, r.UsersWithViolations)
	}
	if r.Lost != 0 {
		t.Fatalf("lost %d", r.Lost)
	}
}

func TestAnalyzeRepeatsAndPoison(t *testing.T) {
	sink := rows("a", 1, 2, 2)
	sink = append(sink, Row{UserID: "a", Seq: 3, Poison: true})
	r := Analyze(sink, nil)
	if r.SeqRepeats != 1 || r.PoisonInSink != 1 || r.OK() {
		t.Fatalf("repeats %d poison %d", r.SeqRepeats, r.PoisonInSink)
	}
}

func TestWrite(t *testing.T) {
	r := Analyze(rows("a", 1, 3), nil)
	r.Lag = -1
	r.ByConsumer = map[string]int64{"consumer-1": 2}
	var b strings.Builder
	now := time.Date(2026, 11, 13, 18, 4, 5, 0, time.FixedZone("MSK", 3*3600))
	if err := Write(&b, "DenisBorzenkov", now, r); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	first, _, _ := strings.Cut(out, "\n")
	if first != "DenisBorzenkov Fri Nov 13 18:04:05 MSK 2026" {
		t.Fatalf("first line %q", first)
	}
	for _, s := range []string{"consumer-1", "a seq 2", "FAIL: потеряно 1"} {
		if !strings.Contains(out, s) {
			t.Fatalf("no %q in\n%s", s, out)
		}
	}
}
