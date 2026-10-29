WITH successful AS (
    SELECT payment_date, sum(amount) AS revenue
    FROM fact_payments
    WHERE payment_status = 'succeeded'
      AND payment_date IS NOT NULL
    GROUP BY payment_date
),
bounds AS (
    SELECT min(payment_date) AS min_day, max(payment_date) AS max_day
    FROM successful
),
spine AS (
    SELECT unnest(
        generate_series(bounds.min_day, bounds.max_day, INTERVAL 1 DAY)
    )::DATE AS revenue_date
    FROM bounds
    WHERE bounds.min_day IS NOT NULL
)
SELECT
    spine.revenue_date,
    sum(coalesce(successful.revenue, 0)) OVER (
        ORDER BY spine.revenue_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS revenue_7d
FROM spine
LEFT JOIN successful ON successful.payment_date = spine.revenue_date
ORDER BY spine.revenue_date
