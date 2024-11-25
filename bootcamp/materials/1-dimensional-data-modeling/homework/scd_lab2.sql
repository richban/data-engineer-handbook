CREATE TABLE players_scd_table (
	player_name text,
	scoring_class scoring_class,
	is_active boolean,
	start_season integer,
	end_date integer,
	current_season INTEGER,
	PRIMARY KEY(player_name, current_season)
);

 CREATE TYPE season_stats AS (
                         season Integer,
                         pts REAL,
                         ast REAL,
                         reb REAL,
                         weight INTEGER
                       );
 CREATE TYPE scoring_class AS
     ENUM ('bad', 'average', 'good', 'star');


 CREATE TABLE players (
     player_name TEXT,
     height TEXT,
     college TEXT,
     country TEXT,
     draft_year TEXT,
     draft_round TEXT,
     draft_number TEXT,
     seasons season_stats[],
     scoring_class scoring_class,
     is_active BOOLEAN,
     current_season INTEGER,
     PRIMARY KEY (player_name, current_season)
 );


WITH last_season AS (
    SELECT * FROM players
    WHERE current_season = 2004

), this_season AS (
     SELECT * FROM player_seasons
    WHERE season = 2005
)
INSERT INTO players
SELECT
        COALESCE(ls.player_name, ts.player_name) as player_name,
        COALESCE(ls.height, ts.height) as height,
        COALESCE(ls.college, ts.college) as college,
        COALESCE(ls.country, ts.country) as country,
        COALESCE(ls.draft_year, ts.draft_year) as draft_year,
        COALESCE(ls.draft_round, ts.draft_round) as draft_round,
        COALESCE(ls.draft_number, ts.draft_number)
            as draft_number,
        COALESCE(ls.seasons,
            ARRAY[]::season_stats[]
            ) || CASE WHEN ts.season IS NOT NULL THEN
                ARRAY[ROW(
                ts.season,
                ts.pts,
                ts.ast,
                ts.reb, ts.weight)::season_stats]
                ELSE ARRAY[]::season_stats[] END
            as seasons,
         CASE
             WHEN ts.season IS NOT NULL THEN
                 (CASE WHEN ts.pts > 20 THEN 'star'
                    WHEN ts.pts > 15 THEN 'good'
                    WHEN ts.pts > 10 THEN 'average'
                    ELSE 'bad' END)::scoring_class
             ELSE ls.scoring_class
         END as scoring_class,
         ts.season IS NOT NULL as is_active,
         2005 AS current_season

    FROM last_season ls
    FULL OUTER JOIN this_season ts
    ON ls.player_name = ts.player_name;


WITH with_previous AS (
	SELECT
		player_name,
		current_season,
		scoring_class,
		is_active,
		LAG(scoring_class,
			1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
		LAG(is_active,
		1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
	FROM
		players
),
with_indicators AS (
SELECT
	*,
	CASE WHEN scoring_class <> previous_scoring_class THEN
		1
	WHEN is_active <> previous_is_active THEN
		1
	ELSE
		0
	END AS change_indicator
FROM
	with_previous
),
with_streaks AS (
SELECT
	*,
	SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
FROM
	with_indicators
),
aggregated AS (
	SELECT
		player_name,
		scoring_class,
		streak_identifier,
		MIN(current_season) AS start_date,
		MAX(current_season) AS end_date
	FROM
		with_streaks
	GROUP BY
		player_name,
		scoring_class,
		streak_identifier
)
SELECT
	player_name,
	scoring_class,
	start_date,
	end_date
FROM
	aggregated;