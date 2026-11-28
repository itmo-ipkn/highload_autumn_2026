package main

import (
	"fmt"
	"strconv"
	"strings"
)

const sinkQuery = `
WITH snapshot AS (
    SELECT 'event' AS kind, user_id, seq, event_id::text, sink_order,
           COALESCE(payload->'poison' = 'true'::jsonb, false) AS poison
    FROM events
    UNION ALL
    SELECT 'duplicates', COALESCE(sum(duplicate_count), 0)::bigint, NULL, NULL, NULL, false
    FROM consumer_metrics
)
SELECT kind, user_id, seq, event_id, sink_order, poison
FROM snapshot
ORDER BY kind, sink_order NULLS FIRST`

func (s *snapshot) readSink(c compose) error {
	data, err := c.exec("db", "psql", "-X", "-A", "-t", "-F", "\t", "-v", "ON_ERROR_STOP=1",
		"-U", "consumer", "-d", "consumer_db", "-c", sinkQuery)
	if err != nil {
		return err
	}

	return lines(data, func(line string) error {
		fields := strings.Split(line, "\t")
		if len(fields) != 6 {
			return fmt.Errorf("invalid sink row: %q", line)
		}

		user, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return err
		}

		if fields[0] == "duplicates" {
			s.duplicates = user
			return nil
		}

		seq, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || user < 1 || seq < 1 || fields[3] == "" || fields[0] != "event" {
			return fmt.Errorf("invalid sink event: %q", line)
		}

		events := s.add(user, seq)
		s.sink[fields[3]] = struct{}{}

		if fields[4] == "" {
			s.unknown++
		} else {
			events.writes = append(events.writes, seq)
		}

		if fields[5] == "t" {
			s.badSink++
		}
		return nil
	})
}
