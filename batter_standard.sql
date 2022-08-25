-- SQLite
WITH TABLE1 AS (SELECT  
    batter
    ,MLBNAME as "Name"
    ,game_year
    ,p_throws
    ,SUM(CASE WHEN events IS NOT NULL THEN 1 ELSE 0 END) AS "PA"
    ,SUM(CASE WHEN events IN ("walk","hit_by_pitch","sac_bunt","sac_fly") THEN 0 WHEN events IS NULL THEN 0 ELSE 1 END) AS "AB"
    ,SUM(CASE WHEN events = "single" THEN 1 ELSE 0 END) AS "single"
    ,SUM(CASE WHEN events = "double" THEN 1 ELSE 0 END) AS "double"
    ,SUM(CASE WHEN events = "triple" THEN 1 ELSE 0 END) AS "triple"
    ,SUM(CASE WHEN events = "home_run" THEN 1 ELSE 0 END) AS "HR"
    ,SUM(CASE WHEN events IN("hit_by_pitch","walk") THEN 1 ELSE 0 END) AS "BB"
    ,SUM(CASE WHEN events = "strikeout" THEN 1 ELSE 0 END) AS "K"
FROM pitch_data
left join player_id on pitch_data.batter = player_id.MLBID
GROUP BY batter, MLBNAME, game_year, p_throws),

SPLITS AS (
SELECT 
    batter
    ,Name
    ,game_year
    ,p_throws
    ,PA
    ,AB
    ,cast((single + [double] + triple + HR) as float)/cast(AB as float) as 'AVG'
    ,cast((BB + single + [double] + triple + HR) as float)/cast(PA as float) as 'OBP'
    ,cast((single*1+[double]*2+triple*3+HR*4) as float)/cast(AB as float) as 'SLG'
    ,cast((single*1+[double]*2+triple*3+HR*4) as float)/cast(AB as float) - cast((single + [double] + triple + HR) as float)/cast(AB as float)  as 'ISO'
    ,SUM(cast(BB as float)) OVER (PARTITION BY batter, game_year) / SUM(cast(PA as float)) OVER (PARTITION BY batter, game_year) as 'BB_Rate'
    ,SUM(cast(K as float)) OVER (PARTITION BY batter, game_year) / SUM(cast(PA as float)) OVER (PARTITION BY batter, game_year) as 'K_Rate'
    ,single
    ,[double]
    ,triple
    ,BB 
    ,K
FROM TABLE1
)

SELECT 
    batter
    ,Name
    ,SUM(CASE WHEN p_throws = 'R' then PA ELSE 0 END) AS 'PA_R'
    ,SUM(CASE WHEN p_throws = 'L' then PA ELSE 0 END) AS 'PA_L'
    ,SUM(CASE WHEN p_throws = 'R' THEN (.33 * AVG) ELSE 0 END) AS 'AVG_R'
    ,SUM(CASE WHEN p_throws = 'L' THEN (.33 * AVG) ELSE 0 END) AS 'AVG_L'
    ,SUM(CASE WHEN p_throws = 'R' THEN (.33 * ISO) ELSE 0 END) AS 'ISO_R'
    ,SUM(CASE WHEN p_throws = 'L' THEN (.33 * ISO) ELSE 0 END) AS 'ISO_L'
    ,SUM(.33 * BB_Rate)/2 AS 'BB_Rate'
    ,SUM(.33 * K_Rate)/2 AS 'K_Rate'
FROM SPLITS
WHERE game_year > 2018
GROUP BY batter, Name