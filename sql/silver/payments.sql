WITH ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY payment_id
            ORDER BY updated_at DESC, filename DESC
        ) AS _rank
    FROM raw_payments
),
latest AS (
    SELECT * FROM ranked WHERE _rank = 1
)
SELECT
    p.payment_id,
    p.subscription_id,
    p.amount,
    p.currency,
    p.status,
    timezone('UTC', p.paid_at) AS paid_at,
    timezone('UTC', p.created_at) AS created_at,
    timezone('UTC', p.updated_at) AS updated_at,
    regexp_extract(p.filename, 'batch=([0-9a-f]+)/', 1) AS _batch_id,
    CASE
        WHEN p.payment_id IS NULL THEN 'missing payment_id'
        WHEN p.subscription_id IS NULL THEN 'missing subscription_id'
        WHEN p.amount IS NULL OR p.amount < 0 THEN 'invalid amount'
        WHEN p.currency IS NULL OR length(trim(p.currency)) <> 3 THEN 'invalid currency'
        WHEN p.status NOT IN ('succeeded', 'failed', 'refunded', 'pending') THEN 'invalid status'
        WHEN p.created_at IS NULL OR p.updated_at IS NULL THEN 'missing timestamps'
        WHEN p.created_at > p.updated_at THEN 'created_after_updated'
        WHEN p.status = 'succeeded' AND p.paid_at IS NULL THEN 'succeeded_without_paid_at'
        WHEN p.paid_at IS NOT NULL AND p.paid_at > p.updated_at THEN 'paid_after_updated'
        WHEN NOT EXISTS (
            SELECT 1 FROM silver_subscriptions s
            WHERE s.subscription_id = p.subscription_id
        ) THEN 'unknown subscription_id'
    END AS _reject_reason
FROM latest p
