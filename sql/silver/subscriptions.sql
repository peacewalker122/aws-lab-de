WITH ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY subscription_id
            ORDER BY updated_at DESC, filename DESC
        ) AS _rank
    FROM raw_subscriptions
),
latest AS (
    SELECT * FROM ranked WHERE _rank = 1
)
SELECT
    s.subscription_id,
    s.customer_id,
    s.plan_name,
    s.monthly_price,
    s.status,
    timezone('UTC', s.created_at) AS created_at,
    timezone('UTC', s.updated_at) AS updated_at,
    regexp_extract(s.filename, 'batch=([0-9a-f]+)/', 1) AS _batch_id,
    CASE
        WHEN s.subscription_id IS NULL THEN 'missing subscription_id'
        WHEN s.customer_id IS NULL THEN 'missing customer_id'
        WHEN s.plan_name IS NULL OR length(trim(s.plan_name)) = 0 THEN 'missing plan_name'
        WHEN s.monthly_price IS NULL OR s.monthly_price < 0 THEN 'invalid monthly_price'
        WHEN s.status NOT IN ('active', 'paused', 'cancelled') THEN 'invalid status'
        WHEN s.created_at IS NULL OR s.updated_at IS NULL THEN 'missing timestamps'
        WHEN s.created_at > s.updated_at THEN 'created_after_updated'
        WHEN NOT EXISTS (
            SELECT 1 FROM silver_customers c
            WHERE c.customer_id = s.customer_id
        ) THEN 'unknown customer_id'
    END AS _reject_reason
FROM latest s
