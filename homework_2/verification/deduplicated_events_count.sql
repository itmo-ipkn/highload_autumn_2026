SELECT COUNT(*) AS deduplication_count FROM event_processing_results WHERE attempt_count > 1;
