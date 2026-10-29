WITH successful AS (
    SELECT payment_date, sum(amount) AS revenue
    FROM fact_payments
    WHERE payment_status = 'succeeded'
      AND payment_date IS NOT NULL
    GROUP BY payment_date
),
prior_revenue AS (
    SELECT calendar_date, prior_revenue AS revenue
    FROM revenue_trend_seed
    WHERE is_lookback
),
revenue_by_day AS (
    SELECT payment_date AS revenue_date, revenue FROM successful
    UNION ALL
    SELECT calendar_date AS revenue_date, revenue FROM prior_revenue
),
trend AS (
    SELECT
        seed.calendar_date AS revenue_date,
        sum(coalesce(revenue_by_day.revenue, 0)) OVER (
            ORDER BY seed.calendar_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS revenue_7d,
        seed.is_lookback
    FROM revenue_trend_seed AS seed
    LEFT JOIN revenue_by_day ON revenue_by_day.revenue_date = seed.calendar_date
)
SELECT revenue_date, revenue_7d
FROM trend
WHERE NOT is_lookback
ORDER BY revenue_date
