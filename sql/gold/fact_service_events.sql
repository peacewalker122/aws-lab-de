SELECT
    e.event_id,
    c.customer_key,
    CAST(e.event_time AS DATE) AS event_date,
    CAST(date_trunc('month', e.event_time) AS DATE) AS event_month,
    e.event_type,
    e.duration_ms,
    e.status_code
FROM silver_service_events e
JOIN dim_customer c ON c.customer_id = e.customer_id
ORDER BY e.event_id
