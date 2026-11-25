// Package verify считает пропуски seq, повторы и нарушения порядка по sink и DLT.
package verify

import (
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"text/tabwriter"
	"time"
)

// Row - строка sink в порядке записи (events.id).
type Row struct {
	UserID string
	Seq    int64
	Poison bool
}

// Key - событие пользователя.
type Key struct {
	UserID string
	Seq    int64
}

// Report - результат проверки.
type Report struct {
	Users      int
	SinkEvents int
	DLTRecords int
	DLTUnique  int
	// MaxSeq - сумма максимальных seq пользователей, UniqueSeq - сумма числа разных seq
	// (sink + DLT). Без потерь они равны.
	MaxSeq    int64
	UniqueSeq int64
	// Lost - seq из 1..max пользователя, которых нет ни в sink, ни в DLT.
	Lost          int64
	UsersWithLoss int
	LostExamples  []Key
	// SeqRepeats - строки sink с (user_id, seq), который уже был записан с другим event_id.
	SeqRepeats int
	// InBoth - seq, который есть и в sink, и в DLT.
	InBoth int
	// OrderViolations - строки sink, у которых seq меньше максимального seq этого
	// пользователя, записанного раньше (по events.id).
	OrderViolations     int
	UsersWithViolations int
	ViolationExamples   []string
	PoisonInSink        int

	// Заполняются вызывающим из Postgres и Kafka.
	Duplicates  int64
	Lag         int64
	ByConsumer  map[string]int64
	ByProducer  map[string]int64
	ByPartition map[string]int64
}

const maxExamples = 10

type user struct {
	seen    map[int64]bool
	sink    map[int64]bool
	max     int64
	written int64 // максимальный seq, уже записанный в sink
	viol    int
}

// Analyze считает отчёт. rows - строки sink в порядке записи, dlt - события из events.dlt.
func Analyze(rows []Row, dlt []Key) Report {
	users := map[string]*user{}
	get := func(id string) *user {
		u, ok := users[id]
		if !ok {
			u = &user{seen: map[int64]bool{}, sink: map[int64]bool{}}
			users[id] = u
		}
		return u
	}

	var r Report
	r.SinkEvents = len(rows)
	for _, row := range rows {
		u := get(row.UserID)
		if row.Poison {
			r.PoisonInSink++
		}
		if u.sink[row.Seq] {
			r.SeqRepeats++
		}
		u.sink[row.Seq] = true
		u.seen[row.Seq] = true
		u.max = max(u.max, row.Seq)
		if row.Seq < u.written {
			r.OrderViolations++
			u.viol++
			if len(r.ViolationExamples) < maxExamples {
				r.ViolationExamples = append(r.ViolationExamples,
					fmt.Sprintf("%s: seq %d после %d", row.UserID, row.Seq, u.written))
			}
		}
		u.written = max(u.written, row.Seq)
	}

	r.DLTRecords = len(dlt)
	dltSeen := map[Key]bool{}
	for _, k := range dlt {
		if dltSeen[k] {
			continue
		}
		dltSeen[k] = true
		u := get(k.UserID)
		if u.sink[k.Seq] {
			r.InBoth++
		}
		u.seen[k.Seq] = true
		u.max = max(u.max, k.Seq)
	}
	r.DLTUnique = len(dltSeen)

	r.Users = len(users)
	for _, id := range slices.Sorted(maps.Keys(users)) {
		u := users[id]
		r.MaxSeq += u.max
		r.UniqueSeq += int64(len(u.seen))
		if u.viol > 0 {
			r.UsersWithViolations++
		}
		lost := false
		for s := int64(1); s <= u.max; s++ {
			if u.seen[s] {
				continue
			}
			r.Lost++
			lost = true
			if len(r.LostExamples) < maxExamples {
				r.LostExamples = append(r.LostExamples, Key{id, s})
			}
		}
		if lost {
			r.UsersWithLoss++
		}
	}
	return r
}

// OK - нет потерь и poison в sink.
func (r Report) OK() bool { return r.Lost == 0 && r.PoisonInSink == 0 }

// Write печатает отчёт. Первая строка - логин и время.
func Write(w io.Writer, login string, now time.Time, r Report) error {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s\n\n", login, now.Format("Mon Jan _2 15:04:05 MST 2006"))

	lag := "н/д"
	if r.Lag >= 0 {
		lag = fmt.Sprint(r.Lag)
	}
	if err := table(&b, [][2]string{
		{"ПРОВЕРКА", "ЗНАЧЕНИЕ"},
		{"пользователей", fmt.Sprint(r.Users)},
		{"событий в sink", fmt.Sprint(r.SinkEvents)},
		{"записей в events.dlt (уникальных)", fmt.Sprintf("%d (%d)", r.DLTRecords, r.DLTUnique)},
		{"lag группы", lag},
		{"сумма max seq по пользователям", fmt.Sprint(r.MaxSeq)},
		{"уникальных seq в sink + dlt", fmt.Sprint(r.UniqueSeq)},
		{"потеряно seq (пользователей)", fmt.Sprintf("%d (%d)", r.Lost, r.UsersWithLoss)},
		{"отброшено дублей по event_id", fmt.Sprint(r.Duplicates)},
		{"повторов seq с другим event_id", fmt.Sprint(r.SeqRepeats)},
		{"seq и в sink, и в dlt", fmt.Sprint(r.InBoth)},
		{"нарушений порядка (пользователей)", fmt.Sprintf("%d (%d)", r.OrderViolations, r.UsersWithViolations)},
		{"poison в sink", fmt.Sprint(r.PoisonInSink)},
	}); err != nil {
		return err
	}

	for _, t := range []struct {
		title string
		m     map[string]int64
	}{
		{"КОНСЬЮМЕР", r.ByConsumer}, {"ПРОДЮСЕР", r.ByProducer}, {"ПАРТИЦИЯ", r.ByPartition},
	} {
		if len(t.m) == 0 {
			continue
		}
		lines := [][2]string{{t.title, "СОБЫТИЙ В SINK"}}
		for _, k := range slices.Sorted(maps.Keys(t.m)) {
			lines = append(lines, [2]string{k, fmt.Sprint(t.m[k])})
		}
		b.WriteString("\n")
		if err := table(&b, lines); err != nil {
			return err
		}
	}

	if len(r.LostExamples) > 0 {
		b.WriteString("\nпримеры потерь:\n")
		for _, k := range r.LostExamples {
			fmt.Fprintf(&b, "  %s seq %d\n", k.UserID, k.Seq)
		}
	}
	if len(r.ViolationExamples) > 0 {
		b.WriteString("\nпримеры нарушений порядка:\n")
		for _, s := range r.ViolationExamples {
			fmt.Fprintf(&b, "  %s\n", s)
		}
	}

	if r.OK() {
		b.WriteString("\nOK: потерь нет\n")
	} else {
		fmt.Fprintf(&b, "\nFAIL: потеряно %d, poison в sink %d\n", r.Lost, r.PoisonInSink)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// table выравнивает две колонки.
func table(b *strings.Builder, lines [][2]string) error {
	var raw strings.Builder
	for _, l := range lines {
		raw.WriteString(l[0] + "\t" + l[1] + "\n")
	}
	tw := tabwriter.NewWriter(b, 0, 0, 2, ' ', 0)
	if _, err := io.WriteString(tw, raw.String()); err != nil {
		return err
	}
	return tw.Flush()
}
