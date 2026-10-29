WITH successful AS (
    SELECT payment_date, sum(amount) AS revenue
    FROM fact_payments
    WHERE payment_status = 'succeeded'
      AND payment_date IS NOT NULL
    GROUP BY payment_date
),
spine AS (
    SELECT unnest(
        generate_series(
            (SELECT min(payment_date) FROM successful),
            (SELECT max(payment_date) FROM successful),
            INTERVAL 1 DAY
        )
    )::DATE AS revenue_date
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
