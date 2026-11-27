// Package sink persists consumed events in Postgres.
package sink

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"

	"highload/hw2/consumer/internal/event"
)

type Sink struct {
	db *sql.DB
}

type Record struct {
	Event     event.Event
	Consumer  string
	Topic     string
	Partition int32
	Offset    int64
}

func Open(ctx context.Context, databaseURL string) (*Sink, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	db.SetMaxOpenConns(20)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	return &Sink{db: db}, nil
}

func (s *Sink) Close() error { return s.db.Close() }

// Store inserts an event exactly once. Re-deliveries are recorded separately.
func (s *Sink) Store(ctx context.Context, record Record) error {
	e := record.Event
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO processed_events
			(event_id, user_id, seq, event_type, consumer_pod, topic, kafka_partition, kafka_offset, produced_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (event_id) DO NOTHING`,
		e.EventID, e.UserID, int64(e.Seq), string(e.Type), record.Consumer,
		record.Topic, record.Partition, record.Offset, e.ProducedAt,
	)
	if err != nil {
		return fmt.Errorf("insert event %s: %w", e.EventID, err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get insert result for event %s: %w", e.EventID, err)
	}
	if inserted == 1 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO duplicate_deliveries
			(event_id, user_id, seq, consumer_pod, topic, kafka_partition, kafka_offset)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		e.EventID, e.UserID, int64(e.Seq), record.Consumer,
		record.Topic, record.Partition, record.Offset,
	); err != nil {
		return fmt.Errorf("record duplicate event %s: %w", e.EventID, err)
	}
	return nil
}
