SELECT
    event_date,
    count(DISTINCT customer_key) AS active_customers
FROM fact_service_events
GROUP BY event_date
ORDER BY event_date
