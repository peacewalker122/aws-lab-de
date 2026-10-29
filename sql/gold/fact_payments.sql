SELECT
    p.payment_id,
    c.customer_key,
    s.subscription_key,
    p.amount,
    CAST(p.paid_at AS DATE) AS payment_date,
    CAST(date_trunc('month', p.paid_at) AS DATE) AS payment_month,
    p.status AS payment_status
FROM silver_payments p
JOIN silver_subscriptions sub ON sub.subscription_id = p.subscription_id
JOIN dim_customer c ON c.customer_id = sub.customer_id
JOIN dim_subscription s ON s.subscription_id = p.subscription_id
ORDER BY p.payment_id
