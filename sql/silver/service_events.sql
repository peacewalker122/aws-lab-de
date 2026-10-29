WITH ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY event_id
            ORDER BY ingested_at DESC, filename DESC
        ) AS _rank
    FROM raw_service_events
),
latest AS (
    SELECT * FROM ranked WHERE _rank = 1
)
SELECT
    e.event_id,
    e.customer_id,
    e.event_type,
    timezone('UTC', e.event_time) AS event_time,
    timezone('UTC', e.ingested_at) AS ingested_at,
    e.duration_ms,
    e.status_code,
    e.payload,
    regexp_extract(e.filename, 'batch=([0-9a-f]+)/', 1) AS _batch_id,
    CASE
        WHEN e.event_id IS NULL THEN 'missing event_id'
        WHEN e.customer_id IS NULL THEN 'missing customer_id'
        WHEN e.event_type IS NULL OR length(trim(e.event_type)) = 0 THEN 'missing event_type'
        WHEN e.event_time IS NULL OR e.ingested_at IS NULL THEN 'missing timestamps'
        WHEN e.duration_ms IS NOT NULL AND e.duration_ms < 0 THEN 'negative duration_ms'
        WHEN e.status_code IS NOT NULL AND (e.status_code < 100 OR e.status_code > 599) THEN 'invalid status_code'
        WHEN NOT EXISTS (
            SELECT 1 FROM silver_customers c
            WHERE c.customer_id = e.customer_id
        ) THEN 'unknown customer_id'
    END AS _reject_reason
FROM latest e
