WITH cte AS (
    SELECT
        user_id,
        seq,
        created_at,
        CASE
            WHEN LAG(created_at, 1, created_at) OVER(PARTITION BY user_id ORDER BY seq) > created_at THEN 'disordered'
            ELSE 'ordered'
        END AS order
    FROM event_processing_results
)
SELECT COUNT(*) FROM cte WHERE cte.order = 'disordered'
;
