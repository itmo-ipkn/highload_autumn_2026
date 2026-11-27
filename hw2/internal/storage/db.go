package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"kafka-events/internal/model"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) SaveSinkEvent(ctx context.Context, event model.Event, consumerID, topic string, partition int32, offset int64) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	result, err := tx.Exec(ctx, `
		INSERT INTO events_sink (event_id, user_id, seq, consumer_id, topic, partition_id, offset_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (event_id) DO NOTHING`,
		event.EventID, event.UserID, event.Seq, consumerID, topic, partition, offset)
	if err != nil {
		return false, fmt.Errorf("insert sink event: %w", err)
	}

	inserted := result.RowsAffected() == 1
	if !inserted {
		_, err = tx.Exec(ctx, `
			INSERT INTO consumer_stats (consumer_id, duplicates_dropped)
			VALUES ($1, 1)
			ON CONFLICT (consumer_id) DO UPDATE
			SET duplicates_dropped = consumer_stats.duplicates_dropped + 1`, consumerID)
		if err != nil {
			return false, fmt.Errorf("increment duplicate counter: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit sink transaction: %w", err)
	}
	return inserted, nil
}

func (s *Store) SinkEvents(ctx context.Context) ([]model.SinkEvent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT event_id::text, user_id, seq, write_order
		FROM events_sink
		ORDER BY write_order`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]model.SinkEvent, 0)
	for rows.Next() {
		var event model.SinkEvent
		if err := rows.Scan(&event.EventID, &event.UserID, &event.Seq, &event.WriteOrder); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *Store) DuplicateCount(ctx context.Context) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(duplicates_dropped), 0)::bigint
		FROM consumer_stats`).Scan(&count)
	return count, err
}
