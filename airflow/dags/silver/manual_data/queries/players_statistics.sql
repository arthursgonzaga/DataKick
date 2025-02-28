
CREATE TABLE {destination_table_name} AS
SELECT
    Jogador AS player,
    Time AS team,
    "#" AS number,
    Nação AS nationality,
    "Pos." AS position,
    Idade AS age,
    "Min." AS minutes,
    Gols AS goals,
    "Assis." AS assists,
    PB AS progressive_passes,
    PT AS total_passes,
    TC AS tackles,
    CaG AS goals_conceded,
    CrtsA AS yellow_cards,
    CrtV AS red_cards,
    Contatos AS touches,
    Div AS duels,
    Crts AS fouls,
    Bloqueios AS blocks,
    xG AS expected_goals,
    npxG AS non_penalty_expected_goals,
    xAG AS expected_assists,
    SCA AS shot_creating_actions,
    GCA AS goal_creating_actions,
    Cmp AS passes_completed,
    Att AS passes_attempted,
    "Cmp%" AS pass_completion_percentage,
    PrgP AS progressive_passes_distance,
    Conduções AS dribbles,
    PrgC AS progressive_carries,
    Tent AS dribbles_attempted,
    Suc AS dribbles_successful,
    Data AS updated_date
FROM read_csv_auto('{s3_path}');