SELECT
    event_date,
    count(*) FILTER (WHERE duration_ms IS NOT NULL) AS measured_events,
    quantile_cont(duration_ms, 0.95) AS p95_duration_ms
FROM fact_service_events
GROUP BY event_date
ORDER BY event_date
