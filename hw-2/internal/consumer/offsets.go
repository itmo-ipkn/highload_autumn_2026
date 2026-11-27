package consumer

import (
	"maps"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Offsets - обработанные, но ещё не закоммиченные оффсеты по партициям одного топика.
// В коммит попадает только то, что прошло через Mark, то есть записано в sink или DLT.
// Безопасен для вызова из колбэков ребаланса.
type Offsets struct {
	mu sync.Mutex
	m  map[int32]kgo.EpochOffset
}

// NewOffsets создаёт пустой учёт.
func NewOffsets() *Offsets { return &Offsets{m: map[int32]kgo.EpochOffset{}} }

// Mark отмечает запись обработанной: следующий коммит партиции будет offset+1.
func (o *Offsets) Mark(r *kgo.Record) {
	o.mu.Lock()
	defer o.mu.Unlock()
	next := kgo.EpochOffset{Epoch: r.LeaderEpoch, Offset: r.Offset + 1}
	if cur, ok := o.m[r.Partition]; !ok || cur.Offset < next.Offset {
		o.m[r.Partition] = next
	}
}

// Pending - копия ещё не закоммиченных оффсетов. Если partitions не пустой - только по ним.
func (o *Offsets) Pending(partitions ...int32) map[int32]kgo.EpochOffset {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(partitions) == 0 {
		return maps.Clone(o.m)
	}
	out := map[int32]kgo.EpochOffset{}
	for _, p := range partitions {
		if eo, ok := o.m[p]; ok {
			out[p] = eo
		}
	}
	return out
}

// Committed убирает закоммиченные оффсеты, если после коммита по партиции не было новых Mark.
func (o *Offsets) Committed(done map[int32]kgo.EpochOffset) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for p, eo := range done {
		if cur, ok := o.m[p]; ok && cur.Offset <= eo.Offset {
			delete(o.m, p)
		}
	}
}

// Drop забывает партиции без коммита (партиции потеряны).
func (o *Offsets) Drop(partitions ...int32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, p := range partitions {
		delete(o.m, p)
	}
}
