-- SQLite

with game_totals as (
SELECT 
 game_date
,game_pk
,home_team as 'Team'
    ,cast(SUM(CASE WHEN events IS NOT NULL THEN 1 ELSE 0 END) as float) AS "PA"
    ,cast(SUM(CASE WHEN events IN ("walk","hit_by_pitch","sac_bunt","sac_fly") THEN 0 WHEN events IS NULL THEN 0 ELSE 1 END)as float) AS "AB"
    ,cast(SUM(CASE WHEN events = "single" THEN 1 ELSE 0 END) as float) AS "single"
    ,cast(SUM(CASE WHEN events = "double" THEN 1 ELSE 0 END) as float) AS "double"
    ,cast(SUM(CASE WHEN events = "triple" THEN 1 ELSE 0 END) as float) AS "triple"
    ,cast(SUM(CASE WHEN events = "home_run" THEN 1 ELSE 0 END) as float) AS "HR"
    ,cast(SUM(CASE WHEN events IN ('single','double','triple','home_run') THEN 1 ELSE 0 END) as float) AS "H"
    ,cast(SUM(CASE WHEN events IN("hit_by_pitch","walk") THEN 1 ELSE 0 END) as float) AS "BB"
    ,cast(SUM(CASE WHEN events = "strikeout" THEN 1 ELSE 0 END) as float) AS "K"
    ,sum(woba_value) as "woba"
,max(post_away_score) as 'runs_allowed'
FROM pitch_data
WHERE inning_topbot = 'Top'
group by game_pk, game_date, home_team

UNION

SELECT 
 game_date
,game_pk
,away_team as 'Team'
    ,cast(SUM(CASE WHEN events IS NOT NULL THEN 1 ELSE 0 END) as float) AS "PA"
    ,cast(SUM(CASE WHEN events IN ("walk","hit_by_pitch","sac_bunt","sac_fly") THEN 0 WHEN events IS NULL THEN 0 ELSE 1 END)as float) AS "AB"
    ,cast(SUM(CASE WHEN events = "single" THEN 1 ELSE 0 END) as float) AS "single"
    ,cast(SUM(CASE WHEN events = "double" THEN 1 ELSE 0 END) as float) AS "double"
    ,cast(SUM(CASE WHEN events = "triple" THEN 1 ELSE 0 END) as float) AS "triple"
    ,cast(SUM(CASE WHEN events = "home_run" THEN 1 ELSE 0 END) as float) AS "HR"
    ,cast(SUM(CASE WHEN events IN ('single','double','triple','home_run') THEN 1 ELSE 0 END) as float) AS "H"
    ,cast(SUM(CASE WHEN events IN("hit_by_pitch","walk") THEN 1 ELSE 0 END) as float) AS "BB"
    ,cast(SUM(CASE WHEN events = "strikeout" THEN 1 ELSE 0 END) as float) AS "K"
    ,sum(woba_value) as "woba"
,max(post_home_score) as 'runs_allowed'
FROM pitch_data
WHERE inning_topbot = 'Bot'
group by game_pk, game_date, away_team)

SELECT
Team
,sum(runs_allowed) / count(game_pk) as 'RA/G'
,SUM(H) / SUM(AB) AS 'BA'
,(SUM(H)+SUM(BB)) / SUM(PA) AS 'OBP'
,(SUM(single) + SUM(double*2) + SUM(triple*3) + SUM(HR*4)) / SUM(AB) AS 'SLG'
,((SUM(H)+SUM(BB)) / SUM(PA)) + ((SUM(single) + SUM(double*2) + SUM(triple*3) + SUM(HR*4)) / SUM(AB)) AS 'OPS'
,SUM(BB) / SUM(PA) AS 'BB_Rate'
,SUM(K) / SUM(PA) AS 'K_Rate'
,sum(woba) / sum(PA) AS 'woba'
FROM game_totals
GROUP BY Team
ORDER BY sum(runs_allowed) / count(game_pk) asc





