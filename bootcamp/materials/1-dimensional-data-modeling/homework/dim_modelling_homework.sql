DROP TYPE IF EXISTS film_struct CASCADE;

CREATE TYPE scoring_class AS ENUM ( 'bad',
	'average',
	'good',
	'star'
);

CREATE TYPE film_struct AS (
	film TEXT,
	votes INT,
	rating NUMERIC(3, 1),
	filmid TEXT
);

DROP TABLE IF EXISTS actors CASCADE;

CREATE TABLE actors (
	actorid TEXT, 
	actor TEXT,
	films film_struct [],
	quality_class scoring_class,
	is_active BOOLEAN,
	current_year INTEGER,
	PRIMARY KEY (actorid, current_year)
);

DROP TABLE IF EXISTS actors_history_scd CASCADE;

CREATE TABLE actors_history_scd (
	actorid text,
	actor text,
	quality_class scoring_class,
	is_active boolean,
	start_date integer,
	end_date integer,
	record_date INTEGER,
	PRIMARY KEY (actorid, start_date)
);


SELECT MIN(year) FROM actor_films;


WITH last_year AS (
    -- Select the most recent data from the `actors` table
    SELECT
        actorid,
        actor,
        films,
        quality_class,
        is_active,
        current_year
    FROM actors
    WHERE current_year = 1976 -- Replace with the previous year being processed
),
this_year AS (
    -- Aggregate data for the current year from `actor_films`
    SELECT
        actorid,
        actor,
        ARRAY_AGG(ROW(film, votes, rating, filmid)::film_struct) AS films,
        CASE 
            WHEN AVG(COALESCE(rating, 0)) > 8 THEN 'star'
            WHEN AVG(COALESCE(rating, 0)) > 7 THEN 'good'
            WHEN AVG(COALESCE(rating, 0)) > 6 THEN 'average'
            ELSE 'bad'
        END::scoring_class AS quality_class,
        -- Set is_active to TRUE only if films array is not null or empty
        CASE 
            WHEN ARRAY_LENGTH(ARRAY_AGG(ROW(film, votes, rating, filmid)::film_struct), 1) > 0 THEN TRUE
            ELSE FALSE
        END AS is_active,
        year AS current_year
    FROM actor_films
    WHERE year = 1977 -- Replace with the current year being processed
    GROUP BY actorid, actor, year
)
INSERT INTO actors
SELECT
    -- Use data from this year if available; otherwise, use last year's data
    COALESCE(ly.actorid, ty.actorid) AS actorid,
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.films, ARRAY[]::film_struct[]) || 
    COALESCE(ty.films, ARRAY[]::film_struct[]) AS films,
    -- Determine quality_class based on this year's data, or carry over last year's
    COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
    -- Determine if the actor is active this year
    COALESCE(ty.is_active, ly.is_active) AS is_active,
    -- Set the current year being processed
    COALESCE(ty.current_year, ly.current_year + 1) AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty
ON ly.actorid = ty.actorid;


-- SCD
WITH actors_previous_year_quality AS (
	SELECT
		actorid,
		actor,
		quality_class,
		is_active,
		current_year,
		LAG(quality_class,
			1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class,
		LAG(is_active,
		1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active
	FROM
		actors
),
with_indicators AS (
SELECT
	*,
	-- Mark rows where the quality_class or is_active changes or it's the first year for the actor
	CASE WHEN quality_class != previous_quality_class
		OR is_active != previous_is_active
		-- OR previous_quality_class IS NULL 
	THEN
		1
	ELSE
		0
	END AS change_indicator
FROM
	actors_previous_year_quality
),
with_streaks AS (
SELECT
	*,
	SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
FROM
	with_indicators
),
aggregated AS (
SELECT
	actorid,
	actor,
	is_active,
	quality_class,
	streak_identifier,
	MIN(current_year) AS start_date,
	MAX(current_year) AS end_date,
	(SELECT MAX(current_year) FROM actors) AS record_date -- Metadata for the last year this record was created
FROM
	with_streaks
GROUP BY
	actorid,
	actor,
	streak_identifier,
	is_active,
	quality_class ORDER BY
	actorid,
	streak_identifier
) INSERT INTO actors_history_scd (actorid, actor, quality_class, is_active, start_date, end_date, record_date)
SELECT
	actorid,
	actor,
	quality_class,
	is_active,
	start_date,
	end_date,
	record_date
FROM
	aggregated;


WITH grouped_data AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        current_year AS start_date,
        LEAD(current_year) OVER (
            PARTITION BY actorid 
            ORDER BY current_year
        ) AS next_year,
        LAG(quality_class) OVER (
            PARTITION BY actorid 
            ORDER BY current_year
        ) AS prev_quality_class,
        LAG(is_active) OVER (
            PARTITION BY actorid 
            ORDER BY current_year
        ) AS prev_is_active
    FROM actors
),
filtered_data AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        start_date,
        next_year,
        -- Mark rows where the quality_class or is_active changes or it's the first year for the actor
        CASE 
            WHEN quality_class != prev_quality_class OR is_active != prev_is_active OR prev_quality_class IS NULL THEN 1
            ELSE 0
        END AS change_flag
    FROM grouped_data
),
aggregated_data AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        MIN(start_date) AS start_date,
        CASE 
            WHEN MAX(next_year) IS NOT NULL THEN MAX(next_year) - 1
            ELSE NULL
        END AS end_date,
        MAX(start_date) AS current_year -- Metadata for the last year this record was created
    FROM filtered_data
    WHERE change_flag = 1 -- Only include rows where changes occurred
    GROUP BY actorid, actor, quality_class, is_active
)
INSERT INTO actors_history_scd (actorid, actor, quality_class, is_active, start_date, end_date, current_year)
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    start_date,
    end_date,
    current_year
FROM aggregated_data;


DROP TYPE IF EXISTS scd_type;

CREATE TYPE scd_type AS (
                    quality_class scoring_class,
                    is_active boolean,
                    start_date INTEGER,
                    end_date INTEGER
                  );

WITH last_year_scd AS (
	SELECT
		*
	FROM
		actors_history_scd
	WHERE
		record_date = 1976
		AND end_date = 1976
),
historical_scd AS (
	SELECT
		actorid,
		actor,
		quality_class,
		is_active,
		start_date,
		end_date
	FROM
		actors_history_scd
	WHERE
		record_date = 1976
		AND end_date < 1976
),
this_year_data AS (
	SELECT
		*
	FROM
		actors
	WHERE
		current_year = 1977
),
unchanged_records AS (
	SELECT
		ty.actorid,
		ty.actor,
		ty.quality_class,
		ty.is_active,
		ly.start_date,
		ty.current_year AS end_date
	FROM
		this_year_data ty
		JOIN last_year_scd ly ON ly.actorid = ty.actorid
	WHERE
		ty.quality_class = ly.quality_class
		AND ty.is_active = ly.is_active
),
changed_records AS (
	SELECT
		ty.actorid,
		ty.actor,
		UNNEST(ARRAY [
                       ROW(
                           ly.quality_class,
                           ly.is_active,
                           ly.start_date,
                           ly.end_date
   
                           )::scd_type,
                       ROW(
                           ty.quality_class,
                           ty.is_active,
                           ty.current_year,
                           ty.current_year
                           )::scd_type
                   ]) AS records
	FROM
		this_year_data ty
	LEFT JOIN last_year_scd ly ON ly.actorid = ty.actorid
WHERE (ty.quality_class <> ly.quality_class
	OR ty.is_active <> ly.is_active)
),
unnested_changed_records AS (
	SELECT
		actorid,
		actor,
		(records::scd_type).quality_class,
		(records::scd_type).is_active,
		(records::scd_type).start_date,
		(records::scd_type).end_date
	FROM
		changed_records
),
new_records AS (
	SELECT
		ty.actorid,
		ty.actor,
		ty.quality_class,
		ty.is_active,
		ty.current_year AS start_date,
		ty.current_year AS end_date
	FROM
		this_year_data ty
	LEFT JOIN last_year_scd ly ON ty.actorid = ly.actorid
WHERE
	ly.actorid IS NULL
) INSERT INTO actors_history_scd (actorid, actor, quality_class, is_active, start_date, end_date, record_date)
SELECT
	*, 1997 AS record_date
FROM (
	SELECT
		*
	FROM
		unchanged_records
	UNION ALL
	SELECT
		*
	FROM
		unnested_changed_records
	UNION ALL
	SELECT
		*
	FROM
		new_records) final_data
WHERE
	NOT EXISTS (
		-- Avoid inserting duplicates
		SELECT
			1
		FROM
			actors_history_scd scd
		WHERE
			scd.actorid = final_data.actorid
			AND scd.start_date = final_data.start_date);
		