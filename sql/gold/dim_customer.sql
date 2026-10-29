SELECT
    row_number() OVER (ORDER BY customer_id) AS customer_key,
    customer_id,
    region,
    CAST(created_at AS DATE) AS created_date
FROM silver_customers
