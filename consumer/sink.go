package main

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type Sink struct {
	conn *pgx.Conn
}

func NewSink(ctx context.Context, dsn string) (*Sink, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}

	return &Sink{conn: conn}, nil
}

func (s *Sink) Close(ctx context.Context) error {
	return s.conn.Close(ctx)
}

func (s *Sink) SaveEvent(
	ctx context.Context,
	event Event,
	consumerID string,
	topic string,
	partition int,
	offset int64,
) (bool, error) {
	tag, err := s.conn.Exec(ctx, `
		INSERT INTO processed_events (
			event_id, user_id, seq, consumer_id,
			topic, partition_id, offset_id
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (event_id) DO NOTHING
	`,
		event.EventID,
		event.UserID,
		event.Seq,
		consumerID,
		topic,
		partition,
		offset,
	)
	if err != nil {
		return false, err
	}

	return tag.RowsAffected() == 1, nil
}

func (s *Sink) IncrementDuplicates(ctx context.Context, consumerID string) error {
	_, err := s.conn.Exec(ctx, `
		INSERT INTO consumer_stats (consumer_id, duplicates_count)
		VALUES ($1, 1)
		ON CONFLICT (consumer_id)
		DO UPDATE SET duplicates_count = consumer_stats.duplicates_count + 1
	`, consumerID)

	return err
}
