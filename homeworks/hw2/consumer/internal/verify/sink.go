package verify

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

type sequence struct {
	user string
	num  uint64
}

type sinkSnapshot struct {
	events          map[uuid.UUID]sequence
	duplicates      int64
	orderViolations int64
}

func readSink(ctx context.Context, db *sql.DB) (sinkSnapshot, error) {
	rows, err := db.QueryContext(ctx, `SELECT event_id, user_id, seq FROM processed_events`)
	if err != nil {
		return sinkSnapshot{}, fmt.Errorf("query processed events: %w", err)
	}
	defer rows.Close()
	sink := sinkSnapshot{events: make(map[uuid.UUID]sequence)}
	for rows.Next() {
		var id uuid.UUID
		var seq sequence
		if err := rows.Scan(&id, &seq.user, &seq.num); err != nil {
			return sinkSnapshot{}, fmt.Errorf("scan processed event: %w", err)
		}
		sink.events[id] = seq
	}
	if err := rows.Err(); err != nil {
		return sinkSnapshot{}, fmt.Errorf("iterate processed events: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM duplicate_deliveries`).Scan(&sink.duplicates); err != nil {
		return sinkSnapshot{}, fmt.Errorf("count duplicate deliveries: %w", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM (
			SELECT seq, max(seq) OVER (
				PARTITION BY user_id ORDER BY write_id
				ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
			) AS previous_max
			FROM processed_events
		) ordered
		WHERE previous_max IS NOT NULL AND seq < previous_max`).Scan(&sink.orderViolations); err != nil {
		return sinkSnapshot{}, fmt.Errorf("check processing order: %w", err)
	}
	return sink, nil
}
