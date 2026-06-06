# duckdb warehouse/lingua.duckdb

-SELECT count(\*) FILTER (WHERE event_type='lesson_completed') FROM raw.events;

-SELECT count(DISTINCT user_id) FILTER (WHERE event_type='lesson_completed') FROM raw.events;

-SELECT round(avg(dau_completed), 2) AS average_dau
FROM (
SELECT count(DISTINCT user_id) FILTER (WHERE event_type='lesson_completed') AS dau_completed
FROM raw.events
GROUP BY date_trunc('day', event_ts)
);
