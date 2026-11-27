WITH cte_inner AS (
    SELECT user_id, MIN(seq) AS start_seq, MAX(seq) AS end_seq, COUNT(*) AS total FROM event_processing_results GROUP BY user_id
),
cte_outer AS (
    SELECT user_id, end_seq - start_seq + 1 AS must_be, total FROM cte_inner
)
SELECT count(*) AS seq_gap_count FROM cte_outer WHERE must_be != total
;
