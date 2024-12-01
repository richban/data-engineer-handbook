-- A query to deduplicate game_details from Day 1 so there's no duplicates

WITH deduped AS (
	SELECT
		g.game_date_est,
		gd.*,
		ROW_NUMBER() OVER (PARTITION BY gd.game_id,
			gd.team_id,
			gd.player_id ORDER BY g.game_date_est) AS row_num
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

-- A DDL for an `user_devices_cumulated` table that has:
-- a `device_activity_datelist` which tracks a users active days by `browser_type`
-- data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
-- or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

DROP TABLE IF EXISTS user_devices_cumulated;

CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    device_activity_datelist JSONB,
    date_last_updated DATE
    PRIMARY KEY (user_id, date_last_updated)
);

-- A cumulative query to generate `device_activity_datelist` from `events`

WITH yesterday_events AS (
	SELECT
		*
	FROM
		user_devices_cumulated
	WHERE
		date_last_updated = DATE('2022-12-31')
),
today_events_sanitized AS (
    SELECT
        e.user_id,
        e.device_id,
        d.browser_type,
        ARRAY_AGG(DISTINCT DATE(e.event_time)) AS event_date
    FROM
        events e
        JOIN devices d ON e.device_id = d.device_id
    WHERE
        e.device_id IS NOT NULL AND e.user_id IS NOT NULL
        -- AND DATE(e.event_time) = DATE('2023-01-01')
    GROUP BY
        e.user_id,
        e.device_id,
        d.browser_type
),
date_series AS (
	SELECT
		generate_series(DATE('2023-01-01'),
			DATE('2023-01-31'),
			'1 day'::interval) AS series_date
),
place_holder_hints AS (
    SELECT
        CASE WHEN event_date @> ARRAY [DATE(series_date)] THEN
        POW(2, 32 - (DATE_PART('day', DATE('2023-01-31')::timestamp - series_date::timestamp)))
        ELSE
            0
        END AS browser_type_date_bit, *
    FROM
        today_events_sanitized
        CROSS JOIN date_series
),
activity_bitmask AS (
    SELECT
        user_id,
        browser_type,
        CAST(CAST(SUM(browser_type_date_bit) AS BIGINT) AS BIT(32)) AS activity_bitmask
        FROM place_holder_hints
        GROUP BY user_id, browser_type
),
json_aggregated AS (
    SELECT
        user_id,
        jsonb_object_agg(browser_type, activity_bitmask::text) AS device_activity_datelist
    FROM
        activity_bitmask
    GROUP BY
        user_id
)
INSERT INTO user_devices_cumulated (user_id, device_activity_datelist, date_last_updated)
SELECT
    *,
    DATE('2023-01-31') AS date_last_updated
FROM
    json_aggregated
ON CONFLICT (user_id, date_last_updated)
DO UPDATE SET
    device_activity_datelist = EXCLUDED.device_activity_datelist,
    date_last_updated = EXCLUDED.date_last_updated;

-- A query to convert `device_activity_datelist` into `datelist_int`

WITH extracted_data AS (
    SELECT
        user_id,
        key AS browser_type,
        value AS bitmask_str
    FROM
        user_devices_cumulated,
        LATERAL jsonb_each_text(device_activity_datelist)
),
converted_to_int AS (
    SELECT
        user_id,
        browser_type,
        CAST(bitmask_str AS BIT(32))::BIGINT AS bitmask_int
    FROM
        extracted_data
),
aggregated_int AS (
    SELECT
        user_id,
        SUM(bitmask_int) AS datelist_int
    FROM
        converted_to_int
    GROUP BY
        user_id
)
SELECT
    user_id,
    datelist_int
FROM
    aggregated_int;

-- DDL for `hosts_cumulated` table
DROP TABLE IF EXISTS hosts_cumulated;

CREATE TABLE hosts_cumulated (
    host_id TEXT,
    host_activity_datelist JSONB,
    date_last_updated DATE,
    PRIMARY KEY (host_id, date_last_updated)
);

WITH load_date_cte AS (
    SELECT DATE('2023-01-01') AS load_date  -- Use a parameter for the date
),
daily_data AS (
    SELECT
        DATE_TRUNC('month', DATE(e.event_time::TIMESTAMP)) AS month,
        e.host,
        COUNT(1) AS daily_hits,
        COUNT(DISTINCT e.user_id) AS daily_unique_visitors
    FROM
        events e,
        load_date_cte
    WHERE
        e.host IS NOT NULL
        AND e.user_id IS NOT NULL
        AND DATE(e.event_time) = load_date_cte.load_date
    GROUP BY
        DATE_TRUNC('month', DATE(e.event_time::TIMESTAMP)),
        e.host
),
all_hosts AS (
    SELECT DISTINCT host FROM host_activity_reduced
    UNION
    SELECT DISTINCT host FROM daily_data
),
combined_data AS (
    SELECT
        ah.host,
        COALESCE(dd.month, DATE_TRUNC('month', load_date_cte.load_date)) AS month,
        COALESCE(dd.daily_hits, 0) AS daily_hits,
        COALESCE(dd.daily_unique_visitors, 0) AS daily_unique_visitors
    FROM
        all_hosts ah
    LEFT JOIN daily_data dd ON ah.host = dd.host,
        load_date_cte
)
INSERT INTO host_activity_reduced (month, host, hit_array, unique_visitors, load_date)
SELECT
    month,
    host,
    ARRAY[daily_hits] AS hit_array,
    ARRAY[daily_unique_visitors] AS unique_visitors,
    load_date_cte.load_date
FROM
    combined_data
ON CONFLICT (host, load_date)
DO NOTHING;