SELECT
    c.region,
    sum(p.amount) AS revenue,
    count(*) AS payment_count
FROM fact_payments p
JOIN dim_customer c USING (customer_key)
WHERE p.payment_status = 'succeeded'
  AND p.payment_date IS NOT NULL
GROUP BY c.region
ORDER BY c.region
