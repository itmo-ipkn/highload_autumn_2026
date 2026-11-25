// verify сверяет sink и events.dlt: пропуски seq, отброшенные дубли, нарушения порядка.
// Код возврата 1, если есть потери или poison в sink.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"
	_ "time/tzdata"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/env"
	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/event"
	"github.com/DenisBorzenkov/ddia-2026/hw-2/internal/verify"
)

func main() {
	wait := flag.Duration("wait", 0, "wait up to this long for consumer group lag 0 before checking")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ok, err := run(log, *wait)
	if err != nil {
		fmt.Fprintln(os.Stderr, "verify:", err)
		os.Exit(2)
	}
	if !ok {
		os.Exit(1)
	}
}

func run(log *slog.Logger, wait time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), wait+2*time.Minute)
	defer cancel()

	brokers := env.List("KAFKA_BROKERS", "localhost:9092")
	group := env.String("GROUP", "events-processor")
	dltTopic := env.String("DLT_TOPIC", "events.dlt")

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return false, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	lag := waitLag(ctx, adm, group, wait, log)

	pool, err := pgxpool.New(ctx, env.String("POSTGRES_DSN", "postgres://events:events@localhost:5432/events"))
	if err != nil {
		return false, err
	}
	defer pool.Close()

	rows, err := sinkRows(ctx, pool)
	if err != nil {
		return false, fmt.Errorf("sink: %w", err)
	}
	dlt, err := dltKeys(ctx, adm, brokers, dltTopic)
	if err != nil {
		return false, fmt.Errorf("dlt: %w", err)
	}

	r := verify.Analyze(rows, dlt)
	r.Lag = lag
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM duplicates").Scan(&r.Duplicates); err != nil {
		return false, err
	}
	if r.ByConsumer, err = counts(ctx, pool, "consumer_id"); err != nil {
		return false, err
	}
	if r.ByProducer, err = counts(ctx, pool, "'producer-' || producer_id"); err != nil {
		return false, err
	}
	if r.ByPartition, err = counts(ctx, pool, "partition::text"); err != nil {
		return false, err
	}

	if err := verify.Write(os.Stdout, env.String("GITHUB_LOGIN", "DenisBorzenkov"), time.Now(), r); err != nil {
		return false, err
	}
	return r.OK(), nil
}

// waitLag ждёт, пока группа догонит топик: два замера подряд группа Stable и lag 0 по
// всем партициям (verify.Settled). Ждёт не дольше wait, возвращает последний lag
// (-1, если по какой-то партиции lag неизвестен).
func waitLag(ctx context.Context, adm *kadm.Client, group string, wait time.Duration, log *slog.Logger) int64 {
	deadline := time.Now().Add(wait)
	var history []verify.LagState
	for {
		st := measureLag(ctx, adm, group, log)
		history = append(history, st)
		if wait <= 0 || verify.Settled(history) || time.Now().After(deadline) {
			return st.Total
		}
		time.Sleep(2 * time.Second)
	}
}

// measureLag - один замер: состояние группы и lag каждой партиции.
func measureLag(ctx context.Context, adm *kadm.Client, group string, log *slog.Logger) verify.LagState {
	lags, err := adm.Lag(ctx, group)
	if err == nil {
		err = lags.Error()
	}
	if err != nil {
		log.Warn("lag", "err", err)
		return verify.LagState{Total: -1}
	}
	g := lags[group]
	var parts []verify.PartitionLag
	for _, ps := range g.Lag {
		for _, p := range ps {
			if p.Err != nil {
				log.Warn("lag", "topic", p.Topic, "partition", p.Partition, "err", p.Err)
			}
			parts = append(parts, verify.PartitionLag{Lag: p.Lag, Err: p.Err})
		}
	}
	return verify.EvalLag(g.State, parts)
}

func sinkRows(ctx context.Context, pool *pgxpool.Pool) ([]verify.Row, error) {
	q, err := pool.Query(ctx, "SELECT user_id, seq, poison FROM events ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer q.Close()
	var out []verify.Row
	for q.Next() {
		var r verify.Row
		if err := q.Scan(&r.UserID, &r.Seq, &r.Poison); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, q.Err()
}

func counts(ctx context.Context, pool *pgxpool.Pool, expr string) (map[string]int64, error) {
	q, err := pool.Query(ctx, "SELECT "+expr+", count(*) FROM events GROUP BY 1")
	if err != nil {
		return nil, err
	}
	defer q.Close()
	out := map[string]int64{}
	for q.Next() {
		var k string
		var n int64
		if err := q.Scan(&k, &n); err != nil {
			return nil, err
		}
		out[k] = n
	}
	return out, q.Err()
}

// dltKeys читает events.dlt целиком: от начала каждой партиции до high watermark на момент
// старта. Без consumer group, оффсеты не коммитятся.
func dltKeys(ctx context.Context, adm *kadm.Client, brokers []string, topic string) ([]verify.Key, error) {
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	if err := ends.Error(); err != nil {
		return nil, err
	}
	remaining := map[int32]int64{}
	parts := map[int32]kgo.Offset{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			remaining[o.Partition] = o.Offset
			parts[o.Partition] = kgo.NewOffset().AtStart()
		}
	})
	if len(remaining) == 0 {
		return nil, nil
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: parts}))
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	var keys []verify.Key
	for len(remaining) > 0 {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("read %s: %w", topic, err)
		}
		fetches.EachError(func(_ string, p int32, err error) {
			slog.Warn("fetch dlt", "partition", p, "err", err)
		})
		for _, r := range fetches.Records() {
			if r.Offset+1 >= remaining[r.Partition] {
				delete(remaining, r.Partition)
			}
			e, err := event.Decode(r.Value)
			if err != nil && !errors.Is(err, event.ErrPoison) {
				continue // value не событие: потерять по нему seq нельзя, пропускаем
			}
			keys = append(keys, verify.Key{UserID: e.UserID, Seq: e.Seq})
		}
	}
	return keys, nil
}
