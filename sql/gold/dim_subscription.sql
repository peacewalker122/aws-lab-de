SELECT
    row_number() OVER (ORDER BY subscription_id) AS subscription_key,
    subscription_id,
    plan_name,
    monthly_price,
    status
FROM silver_subscriptions
