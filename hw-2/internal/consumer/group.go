package consumer

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Group - цикл консьюмера в группе: poll, обработка, периодический синхронный коммит
// обработанного.
//
// Клиент создаётся с kgo.DisableAutoCommit и kgo.BlockRebalanceOnPoll: пока обрабатывается
// пачка из PollRecords, ребаланс ждёт AllowRebalance. Оффсеты коммитятся только из Offsets,
// куда запись попадает после записи в sink или DLT.
type Group struct {
	Topic   string
	MaxPoll int
	// CommitInterval - как часто коммитить обработанное.
	CommitInterval time.Duration
	// Owner - номер консьюмера, значение метрики consumer_partition_owner для его партиций.
	Owner   float64
	Proc    *Processor
	Offsets *Offsets
	Metrics *Metrics
	Log     *slog.Logger

	mu    sync.Mutex
	fresh map[int32]bool // партиции, назначенные после последнего прочитанного из них
}

// Opts - колбэки ребаланса для kgo.NewClient.
func (g *Group) Opts() []kgo.Opt {
	return []kgo.Opt{
		kgo.OnPartitionsAssigned(g.assigned),
		kgo.OnPartitionsRevoked(g.revoked),
		kgo.OnPartitionsLost(g.lost),
	}
}

func (g *Group) assigned(_ context.Context, _ *kgo.Client, m map[string][]int32) {
	ps := m[g.Topic]
	if len(ps) == 0 {
		return
	}
	g.Metrics.Rebalances.WithLabelValues("assigned").Inc()
	g.mu.Lock()
	if g.fresh == nil {
		g.fresh = map[int32]bool{}
	}
	for _, p := range ps {
		g.fresh[p] = true
		g.Metrics.Assigned.WithLabelValues(strconv.Itoa(int(p))).Set(g.Owner)
	}
	g.mu.Unlock()
	g.Log.Info("partitions assigned", "partitions", ps)
}

// revoked: партиции уходят другому участнику. Всё обработанное по ним коммитится
// синхронно до того, как клиент продолжит ребаланс.
func (g *Group) revoked(ctx context.Context, cl *kgo.Client, m map[string][]int32) {
	ps := m[g.Topic]
	if len(ps) == 0 {
		return
	}
	g.Metrics.Rebalances.WithLabelValues("revoked").Inc()
	pending := g.Offsets.Pending(ps...)
	err := g.commit(ctx, cl, pending)
	g.Log.Info("partitions revoked", "partitions", ps, "committed", offsetsLog(pending), "err", err)
	g.forget(ps)
}

// lost: участник выпал из группы (сессия истекла), партиции уже могут читать другие.
// Коммитить нельзя: оффсеты перезаписали бы коммиты нового владельца.
func (g *Group) lost(_ context.Context, _ *kgo.Client, m map[string][]int32) {
	ps := m[g.Topic]
	if len(ps) == 0 {
		return
	}
	g.Metrics.Rebalances.WithLabelValues("lost").Inc()
	g.Log.Warn("partitions lost, not committing", "partitions", ps, "dropped", offsetsLog(g.Offsets.Pending(ps...)))
	g.forget(ps)
}

func (g *Group) forget(ps []int32) {
	g.Offsets.Drop(ps...)
	g.mu.Lock()
	for _, p := range ps {
		delete(g.fresh, p)
		g.Metrics.Assigned.WithLabelValues(strconv.Itoa(int(p))).Set(0)
	}
	g.mu.Unlock()
}

// commit синхронно коммитит pending и при успехе снимает их из Offsets.
func (g *Group) commit(ctx context.Context, cl *kgo.Client, pending map[int32]kgo.EpochOffset) error {
	if len(pending) == 0 {
		return nil
	}
	var cerr error
	cl.CommitOffsetsSync(ctx, map[string]map[int32]kgo.EpochOffset{g.Topic: pending},
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				cerr = err
				return
			}
			for _, t := range resp.Topics {
				for _, p := range t.Partitions {
					if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
						cerr = errors.Join(cerr, err)
					}
				}
			}
		})
	if cerr != nil {
		g.Metrics.Commits.WithLabelValues("error").Inc()
		g.Log.Error("commit", "offsets", offsetsLog(pending), "err", cerr)
		return cerr
	}
	g.Metrics.Commits.WithLabelValues("ok").Inc()
	g.Offsets.Committed(pending)
	return nil
}

// Run читает и обрабатывает записи, пока ctx не отменён. Обработанное коммитится не
// чаще раза в CommitInterval (и при каждом отзыве партиций - в OnPartitionsRevoked).
// Poll ограничен тем же интервалом, чтобы хвост коммитился и без новых записей.
func (g *Group) Run(ctx context.Context, cl *kgo.Client) {
	last := time.Now()
	for {
		pctx, cancel := context.WithTimeout(ctx, g.CommitInterval)
		fetches := cl.PollRecords(pctx, g.MaxPoll)
		cancel()
		if fetches.IsClientClosed() || ctx.Err() != nil {
			g.Commit(cl)
			cl.AllowRebalance()
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) {
				g.Log.Warn("fetch", "topic", t, "partition", p, "err", err)
			}
		})

		aborted := false
		for _, r := range fetches.Records() {
			g.logResume(r)
			if _, err := g.Proc.Handle(ctx, r); err != nil {
				aborted = true
				break
			}
			g.Offsets.Mark(r)
		}
		if aborted || time.Since(last) >= g.CommitInterval {
			g.Commit(cl)
			last = time.Now()
		}
		cl.AllowRebalance()
		if aborted {
			return
		}
	}
}

// Commit синхронно коммитит всё обработанное. Ошибка коммита не фатальна: оффсеты
// остаются в Offsets и уйдут следующим коммитом.
func (g *Group) Commit(cl *kgo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = g.commit(ctx, cl, g.Offsets.Pending())
}

// logResume пишет в лог первый оффсет, прочитанный из партиции после назначения.
func (g *Group) logResume(r *kgo.Record) {
	g.mu.Lock()
	fresh := g.fresh[r.Partition]
	delete(g.fresh, r.Partition)
	g.mu.Unlock()
	if fresh {
		g.Log.Info("partition resumed", "partition", r.Partition, "offset", r.Offset)
	}
}

func offsetsLog(m map[int32]kgo.EpochOffset) map[string]int64 {
	out := make(map[string]int64, len(m))
	for p, eo := range m {
		out[strconv.Itoa(int(p))] = eo.Offset
	}
	return out
}
