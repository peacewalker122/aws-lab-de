SELECT
    payment_date,
    sum(amount) AS revenue,
    count(*) AS payment_count
FROM fact_payments
WHERE payment_status = 'succeeded'
  AND payment_date IS NOT NULL
GROUP BY payment_date
ORDER BY payment_date
