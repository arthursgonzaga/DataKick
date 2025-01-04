CREATE TABLE {destination_table_name} AS
WITH home_matches AS (
    SELECT 
        home_team AS team, 
        SUM(home_score) AS goals_for,
        SUM(away_score) AS goals_against,
        SUM(CAST(home_passes_completed AS INT)) AS passes_for_completed,
        SUM(CAST(home_passes_failed AS INT)) AS passes_for_failed,
        SUM(CAST(home_shoots_on_goal AS INT)) AS shoots_on_goal,
        SUM(CAST(home_shoots_off_goal AS INT)) AS shoots_off_goal,
        SUM(CAST(home_shoots_on_crossbar AS INT)) AS shoots_crossbar, 
        SUM(CAST(home_shoots_blocked AS INT)) AS shoots_blocked,
        COUNT(home_team) AS matches
    FROM 
        read_parquet('{s3_path}')
    GROUP BY 
        home_team
),
away_matches AS (
    SELECT 
        away_team AS team, 
        SUM(away_score) AS goals_for,
        SUM(home_score) AS goals_against,
        SUM(CAST(away_passes_completed AS INT)) AS passes_for_completed,
        SUM(CAST(away_passes_failed AS INT)) AS passes_for_failed,
        SUM(CAST(away_shoots_on_goal AS INT)) AS shoots_on_goal,
        SUM(CAST(away_shoots_off_goal AS INT)) AS shoots_off_goal,
        SUM(CAST(away_shoots_on_crossbar AS INT)) AS shoots_crossbar, 
        SUM(CAST(away_shoots_blocked AS INT)) AS shoots_blocked,
        COUNT(away_team) AS matches
    FROM 
        read_parquet('{s3_path}')
    GROUP BY 
        away_team
),
joined AS (
    SELECT 
        *, 
        'home' AS tag
    FROM 
        home_matches
    WHERE 
        team IS NOT NULL
    UNION ALL
    SELECT 
        *,
        'away' AS tag
    FROM 
        away_matches
    WHERE 
        team IS NOT NULL
)

SELECT 
    team, 
    SUM(goals_for) AS GF, 
    SUM(goals_against) AS GA,
    SUM(shoots_on_goal) AS sum_shoots_on_goal,
    ROUND(AVG(shoots_on_goal/matches),3) AS avg_shoots_on_goal_by_match,
    SUM(shoots_off_goal) AS sum_shoots_off_goal,
    ROUND(AVG(shoots_off_goal/matches),3) AS avg_shoots_off_goal_by_match,
    SUM(shoots_blocked) AS sum_shoots_blocked,
    ROUND(AVG(shoots_blocked/matches),3) AS avg_shoots_blocked_by_match,
    SUM(shoots_crossbar) AS sum_shoots_crossbar,
    ROUND(AVG(shoots_crossbar/matches),3) AS avg_shoots_crossbar_by_match,
    SUM(passes_for_completed) AS total_passes_completed,
    SUM(passes_for_failed) AS total_passes_failed,
    ROUND(AVG(passes_for_completed/matches),3) AS avg_passes_completed_by_match,
    ROUND(AVG(passes_for_failed/matches),3) AS avg_passes_failed_by_match,
    SUM(passes_for_completed)/(SUM(passes_for_completed)+SUM(passes_for_failed)) AS ratio_pass
FROM 
    joined
GROUP BY 
    team
;