WITH teams_deduped AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id) as row_num
    FROM teams
)
SELECT
    team_id AS identifier,
    'team'::vertex_type AS type,
    json_build_object(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
        )
FROM teams_deduped
WHERE row_num = 1;

WITH games_deduped AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY game_id) as row_num
    FROM games
)
INSERT INTO vertices
SELECT
    game_id AS identifier,
    'game'::vertex_type AS type,
    json_build_object(
        'pts_home', pts_home,
        'pts_away', pts_away,
        'winning_team', CASE 
                            WHEN home_team_wins = 1 THEN home_team_id
                            ELSE visitor_team_id
                        END
    ) AS properties
FROM games;