SELECT
    s.plan_name,
    sum(p.amount) AS revenue,
    count(*) AS payment_count
FROM fact_payments p
JOIN dim_subscription s USING (subscription_key)
WHERE p.payment_status = 'succeeded'
  AND p.payment_date IS NOT NULL
GROUP BY s.plan_name
ORDER BY s.plan_name
