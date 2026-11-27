package generator

import (
	"sort"
	"sync"
)

// Sequencer выдаёт пользователям seq без пропусков. Он безопасен для конкурентного использования.
//
// У каждого пользователя свой мьютекс. Do держит его на всё время публикации, поэтому:
//   - seq увеличивается, только если событие действительно ушло в продюсер;
//   - события одного пользователя попадают во входной канал продюсера в порядке seq,
//     даже если генерирующих воркеров несколько;
//   - разные пользователи публикуются параллельно и не ждут друг друга.
type Sequencer struct {
	mu    sync.RWMutex
	users map[string]*userSeq
}

type userSeq struct {
	mu   sync.Mutex
	last uint64
}

func NewSequencer() *Sequencer {
	return &Sequencer{users: make(map[string]*userSeq)}
}

// Do вызывает fn со следующим seq пользователя. Если fn вернёт nil, seq фиксируется.
// При ошибке (например, отменён контекст на SIGTERM) seq не расходуется: следующий
// вызов получит то же значение, и пропуска не будет.
func (s *Sequencer) Do(userID string, fn func(seq uint64) error) error {
	u := s.user(userID)
	u.mu.Lock()
	defer u.mu.Unlock()

	next := u.last + 1
	if err := fn(next); err != nil {
		return err
	}
	u.last = next
	return nil
}

// Last возвращает последний зафиксированный seq пользователя (0 — событий не было).
func (s *Sequencer) Last(userID string) uint64 {
	s.mu.RLock()
	u, ok := s.users[userID]
	s.mu.RUnlock()
	if !ok {
		return 0
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.last
}

// Snapshot возвращает последние seq всех пользователей, отсортированные по user_id.
func (s *Sequencer) Snapshot() []UserSeq {
	s.mu.RLock()
	ids := make([]string, 0, len(s.users))
	for id := range s.users {
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	sort.Strings(ids)

	out := make([]UserSeq, 0, len(ids))
	for _, id := range ids {
		out = append(out, UserSeq{UserID: id, LastSeq: s.Last(id)})
	}
	return out
}

// UserSeq — последний seq пользователя.
type UserSeq struct {
	UserID  string
	LastSeq uint64
}

func (s *Sequencer) user(id string) *userSeq {
	s.mu.RLock()
	u, ok := s.users[id]
	s.mu.RUnlock()
	if ok {
		return u
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if u, ok = s.users[id]; !ok {
		u = &userSeq{}
		s.users[id] = u
	}
	return u
}
