# duckdb warehouse/lingua.duckdb

-SELECT count(\*) FILTER (WHERE event_type='lesson_completed') FROM raw.events;

-SELECT count(DISTINCT user_id) FILTER (WHERE event_type='lesson_completed') FROM raw.events;

-SELECT round(avg(dau_completed), 2) AS average_dau
FROM (
SELECT count(DISTINCT user_id) FILTER (WHERE event_type='lesson_completed') AS dau_completed
FROM raw.events
GROUP BY date_trunc('day', event_ts)
);

-SELECT u.acquisition_channel,
count(DISTINCT e.user_id) as active_users,
count(\*) FILTER (WHERE e.event_type='lesson_completed') as completed
FROM raw.users u
LEFT JOIN raw.events e ON e.user_id = u.user_id
GROUP BY u.acquisition_channel
ORDER BY active_users DESC;

# Retention

WITH signups AS (
SELECT user_id,
date_trunc('day', signup_ts) AS signup_date
FROM raw.users
),

activity AS (
SELECT DISTINCT user_id,
date_trunc('day', event_ts) AS active_date
FROM raw.events
),

user_days AS (
SELECT a.user_id,
date_diff('day', s.signup_date, a.active_date) AS days_since
FROM activity a
JOIN signups s ON s.user_id = a.user_id
)

SELECT
count(DISTINCT user_id) AS total_users,
count(DISTINCT user_id) FILTER (WHERE days_since = 1) AS "first day",
count(DISTINCT user_id) FILTER (WHERE days_since BETWEEN 1 AND 7) AS "week",
count(DISTINCT user_id) FILTER (WHERE days_since BETWEEN 1 AND 30) AS "month",
round(100.0 _ count(DISTINCT user_id) FILTER (WHERE days_since = 1)
/ count(DISTINCT user_id), 1) AS "first day %",
round(100.0 _ count(DISTINCT user_id) FILTER (WHERE days_since BETWEEN 1 AND 7)
/ count(DISTINCT user_id), 1) AS "week %",
round(100.0 \* count(DISTINCT user_id) FILTER (WHERE days_since BETWEEN 1 AND 30)
/ count(DISTINCT user_id), 1) AS "month %"
FROM user_days;
