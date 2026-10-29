SELECT
    event_date,
    count(*) AS event_count
FROM fact_service_events
GROUP BY event_date
ORDER BY event_date
