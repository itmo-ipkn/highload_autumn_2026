WITH cte AS (
    SELECT user_id, seq, COUNT(*) FROM event_processing_results GROUP BY user_id, seq HAVING COUNT(seq) > 1
)
SELECT COUNT(*) AS duplicated_seq_count FROM cte
;
