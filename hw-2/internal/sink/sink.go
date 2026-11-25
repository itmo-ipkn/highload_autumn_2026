// Package sink - запись обработанных событий в Postgres.
package sink

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

// Row - обработанное событие и откуда оно прочитано.
type Row struct {
	EventID    string
	UserID     string
	Seq        int64
	Type       string
	Poison     bool
	ProducerID string
	ConsumerID string
	Topic      string
	Partition  int32
	Offset     int64
	ProducedAt time.Time
}

// Postgres - sink в Postgres.
type Postgres struct {
	pool *pgxpool.Pool
}

// Open подключается к Postgres и создаёт схему, если её нет.
func Open(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	p := &Postgres{pool: pool}
	if err := p.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return p, nil
}

// Pool - пул соединений, для verify.
func (p *Postgres) Pool() *pgxpool.Pool { return p.pool }

// Close закрывает пул.
func (p *Postgres) Close() { p.pool.Close() }

// migrate применяет schema.sql. Консьюмеры стартуют одновременно, advisory lock не даёт
// им параллельно выполнять CREATE TABLE IF NOT EXISTS.
func (p *Postgres) migrate(ctx context.Context) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // после Commit - no-op
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(20262)"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, schema); err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	return tx.Commit(ctx)
}

// insertSQL - одна команда: вставка события, а если event_id уже есть - строка в duplicates.
const insertSQL = `
WITH ins AS (
    INSERT INTO events (event_id, user_id, seq, type, poison, producer_id, consumer_id,
                        topic, partition, "offset", produced_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    ON CONFLICT (event_id) DO NOTHING
    RETURNING id
)
INSERT INTO duplicates (event_id, consumer_id, topic, partition, "offset")
SELECT $1, $7, $8, $9, $10
WHERE NOT EXISTS (SELECT 1 FROM ins)`

// Insert записывает событие идемпотентно по event_id. duplicate - событие уже было
// записано раньше, повтор учтён в duplicates.
func (p *Postgres) Insert(ctx context.Context, r Row) (duplicate bool, err error) {
	tag, err := p.pool.Exec(ctx, insertSQL, r.EventID, r.UserID, r.Seq, r.Type, r.Poison,
		r.ProducerID, r.ConsumerID, r.Topic, r.Partition, r.Offset, r.ProducedAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
