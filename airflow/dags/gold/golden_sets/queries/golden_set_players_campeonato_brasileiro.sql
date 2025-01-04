CREATE TABLE {destination_table_name} AS
WITH last_position AS (
    SELECT
        player,
        team,
        FIRST_VALUE(position) OVER (PARTITION BY player, team ORDER BY updated_date DESC) AS position,
    FROM 
        read_parquet('{s3_path}')
    GROUP BY player, team, position, updated_date
)



SELECT 
    b.player,
    b.team,
    lp.position,
    COUNT(b.player) AS matches,
    AVG(minutes) AS average_minutes,
    SUM(goals) AS total_goals,
    SUM(assists) AS total_assists,
    SUM(goals) + SUM(assists) AS total_goals_participation,
    SUM(yellow_cards) AS total_yellow_cards,
    SUM(red_cards) AS total_red_cards,
    AVG(expected_goals) AS average_xg,
    AVG(expected_assists) AS average_xa
FROM 
    read_parquet('{s3_path}') AS b
LEFT JOIN last_position AS lp
    ON (b.player = lp.player)
    AND (b.team = lp.team)
GROUP BY b.player, b.team, lp.position