
SELECT 
    player_boxscores.player_id,
    player_boxscores.player,
    player_boxscores.position,
    player_boxscores.team_abbr,
    AVG(player_boxscores.mp) as mpg,
    SAFE_DIVIDE(SUM(player_boxscores.fgm), SUM(player_boxscores.fga)) as fg_pct,
    SAFE_DIVIDE(SUM(player_boxscores.fg3m), SUM(player_boxscores.fg3a)) as fg3_pct,
    SAFE_DIVIDE(SUM(player_boxscores.ftm), SUM(player_boxscores.fta)) as ft_pct,
    AVG(player_boxscores.pts) as ppg,
    AVG(player_boxscores.reb) as rpg,
    AVG(player_boxscores.ast) as apg,
    AVG(player_boxscores.stl) as spg,
    AVG(player_boxscores.blk) as bpg,
    AVG(player_boxscores.tov) as tpg
FROM nba.player_boxscores as player_boxscores
JOIN nba.game_summaries as game_summaries 
ON player_boxscores.game_id = game_summaries.game_id
WHERE 1=1 and 2=2 and game_summaries.season=2024
GROUP BY 1,2,3,4
HAVING 
    SUM(player_boxscores.fga) > 0 AND
    SUM(player_boxscores.fg3a) > 0 AND
    SUM(player_boxscores.fta) > 0
