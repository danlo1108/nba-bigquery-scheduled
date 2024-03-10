
{{ config(materialized='view') }}

SELECT 
  team_abbr,
  AVG(fg_pct) as fg_pct,
  AVG(fg3_pct) as fg3_pct,
  AVG(ft_pct) as ft_pct,
  AVG(ppg) as ppg,
  AVG(rpg) as rpg,
  AVG(apg) as apg,
  AVG(spg) as spg,
  AVG(bpg) as bpg,
  AVG(tpg) as tpg
FROM {{ ref('player_stats_agg') }}
GROUP BY 1