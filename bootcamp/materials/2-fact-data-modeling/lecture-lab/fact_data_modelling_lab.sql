SELECT
game_id, team_id, player_id, COUNT(1)
FROM game_details
GROUP BY 1,2,3
HAVING COUNT (1) > 1;

WITH deduped AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY game_id,
			team_id,
			player_id) AS row_num
	FROM
		game_details
)
SELECT
	*
FROM
	deduped
WHERE
	row_num = 1;



WITH deduped AS (
	SELECT
		g.game_date_est,
		gd.*,
		ROW_NUMBER() OVER (PARTITION BY gd.game_id,
			team_id,
			player_id ORDER BY g.game_date_est) AS row_num
	FROM
		game_details gd
		JOIN games g ON gd.game_id = g.game_id
)
SELECT
	*
FROM
	deduped
WHERE
	row_num = 1;

-- LAB 2

CREATE TABLE users_cumulated (
     user_id TEXT,
     -- The list of dates in the past where the user was active
     dates_active DATE[],
     -- The current date of the user
     date DATE,
     PRIMARY KEY (user_id, date)
 );


INSERT INTO users_cumulated
WITH yesterday AS (
	SELECT
		*
	FROM
		users_cumulated
	WHERE
		date = DATE('2023-01-30')
),
today AS (
	SELECT
		CAST(user_id AS TEXT) as user_id,
		DATE(event_time::TIMESTAMP) AS date_active
	FROM
		events
	WHERE
		DATE(event_time::TIMESTAMP) = DATE('2023-01-31')
		AND user_id IS NOT NULL
	GROUP BY
		user_id,
		DATE(event_time::TIMESTAMP)
)
SELECT
	COALESCE(t.user_id, y.user_id) AS user_id,
	CASE WHEN y.dates_active IS NULL THEN
		ARRAY [t.date_active]
	WHEN t.date_active IS NULL THEN
		y.dates_active
	ELSE
		ARRAY [t.date_active] || y.dates_active
	END AS dates_active,
	COALESCE(t.date_active, y.date + Interval '1 day') AS date
FROM
	today t
	FULL OUTER JOIN yesterday y ON t.user_id = y.user_id;


-- The constant 32 ensures that the values are appropriately scaled, with the difference between dates having an exponential impact on the result.


WITH users AS (
	SELECT
		*
	FROM
		users_cumulated
	WHERE
		date = DATE('2023-01-31')
),
series AS (
	SELECT
		generate_series(DATE('2023-01-01'),
			DATE('2023-01-31'),
			'1 day'::interval) AS series_date
),
place_holder_hints AS (
SELECT
	CASE WHEN dates_active @> ARRAY [DATE(series_date)] THEN
		POW(2, 32 - (date - DATE(series_date)))
	ELSE
		0
	END AS place_holder_int_value,
	*
FROM
	users
	CROSS JOIN series
-- WHERE
-- 	user_id = '8045844334478885000'
)
SELECT
	user_id,
	CAST(CAST(SUM(place_holder_int_value) AS BIGINT) AS BIT(32))
FROM place_holder_hints
GROUP BY user_id
;

CREATE TABLE array_metrics (
	user_id NUMERIC,
	month_start DATE,
	metric_name TEXT,
	metric_array REAL [],
	PRIMARY KEY (user_id, month_start, metric_name)
);

SELECT cardinality(metric_array), COUNT(1)
FROM array_metrics
GROUP BY 1;

INSERT INTO array_metrics WITH yesterday_array AS (
	SELECT
		*
	FROM
		array_metrics
	WHERE
		month_start = DATE('2023-01-01')
),
daily_aggregate AS (
	SELECT
		user_id,
		DATE(event_time) AS date,
		COUNT(1) AS num_site_hits
	FROM
		events
	WHERE
		DATE(event_time) = DATE('2023-01-04')
		AND user_id IS NOT NULL
	GROUP BY
		1,
		2
)
SELECT
	COALESCE(da.user_id, ya.user_id) AS user_id,
	COALESCE(ya.month_start, DATE(DATE_TRUNC('month', da.date))) AS month_start,
	'site_hits' AS metric_name,
	 -- Case where there is already an accumulated array (user exists in both tables)
	CASE WHEN ya.metric_array IS NOT NULL THEN
		ya.metric_array || ARRAY [COALESCE(da.num_site_hits, 0)]
	-- Case where no previous array exists (user exists only in daily_aggregate)
	WHEN ya.metric_array IS NULL THEN
	-- Create an array of leading zeros for days before the user's first activity day
		ARRAY_FILL(0, ARRAY [COALESCE(DATE(da.date) - DATE(DATE_TRUNC('month', da.date)), 0)]) || ARRAY [COALESCE(da.num_site_hits, 0)]
	END AS metric_array
FROM
	daily_aggregate da
	FULL OUTER JOIN yesterday_array ya ON da.user_id = ya.user_id AND
	ya.month_start = DATE_TRUNC('month', da.date)  -- Ensure we're comparing the same month
	 ON CONFLICT (user_id,
	month_start,
	metric_name)
	DO
	UPDATE
	SET
		metric_array = EXCLUDED.metric_array;

WITH agg AS (
	SELECT
		metric_name,
		month_start,
		ARRAY [SUM(metric_array[1]
),
SUM(
	metric_array [2]
),
SUM(
	metric_array [3]
)] AS summed_array
FROM
	array_metrics
GROUP BY
	metric_name,
	month_start
)
SELECT
	metric_name,
	month_start + CAST(CAST(index - 1 AS TEXT) || ' day' AS INTERVAL) as date,
	elem as value
FROM
	agg
	CROSS JOIN unnest(
		agg.summed_array
) WITH ordinality AS a (
		elem,
		index
)