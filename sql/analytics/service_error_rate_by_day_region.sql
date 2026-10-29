SELECT
    f.event_date,
    c.region,
    count(*) AS event_count,
    count(*) FILTER (WHERE f.status_code >= 500) AS error_count,
    (count(*) FILTER (WHERE f.status_code >= 500))::DOUBLE / count(*) AS error_rate
FROM fact_service_events f
JOIN dim_customer c USING (customer_key)
GROUP BY f.event_date, c.region
ORDER BY f.event_date, c.region
