WITH ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at DESC, filename DESC
        ) AS _rank
    FROM raw_customers
),
latest AS (
    SELECT * FROM ranked WHERE _rank = 1
)
SELECT
    customer_id,
    name,
    region,
    timezone('UTC', created_at) AS created_at,
    timezone('UTC', updated_at) AS updated_at,
    regexp_extract(filename, 'batch=([0-9a-f]+)/', 1) AS _batch_id,
    CASE
        WHEN customer_id IS NULL THEN 'missing customer_id'
        WHEN name IS NULL OR length(trim(name)) = 0 THEN 'missing name'
        WHEN region IS NULL OR length(trim(region)) = 0 THEN 'missing region'
        WHEN created_at IS NULL OR updated_at IS NULL THEN 'missing timestamps'
        WHEN created_at > updated_at THEN 'created_after_updated'
    END AS _reject_reason
FROM latest
