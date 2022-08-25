-- !preview conn=DBI::dbConnect(RSQLite::SQLite(),"mlb.db")

WITH TOTALS AS (
SELECT  
    batter
    ,MLBNAME as "Name"
    ,player_ids.TEAM
    ,player_ids.POS
    ,SUM(CASE WHEN events IS NOT NULL THEN 1 ELSE 0 END) AS "PA"
    ,SUM(CASE WHEN events IN ("walk","hit_by_pitch","sac_bunt","sac_fly") THEN 0 WHEN events IS NULL THEN 0 ELSE 1 END) AS "AB"
    ,SUM(CASE WHEN events = "single" THEN 1 ELSE 0 END) AS "single"
    ,SUM(CASE WHEN events = "double" THEN 1 ELSE 0 END) AS "double"
    ,SUM(CASE WHEN events = "triple" THEN 1 ELSE 0 END) AS "triple"
    ,SUM(CASE WHEN events = "home_run" THEN 1 ELSE 0 END) AS "HR"
    ,SUM(CASE WHEN events IN("hit_by_pitch","walk") THEN 1 ELSE 0 END) AS "BB"
    ,SUM(CASE WHEN events = "strikeout" THEN 1 ELSE 0 END) AS "K"
FROM (select distinct * from pitch_data) as pitch_data
LEFT JOIN player_ids on pitch_data.batter = player_ids.MLBID
GROUP BY batter, MLBNAME),

standard as (
SELECT
    batter
    ,Name
    ,Team
    ,POS
    ,PA
    ,AB
    ,HR
    ,cast((single + double + triple + HR)as float) / cast(AB as float) as 'AVG'
    ,cast((single + double + triple + HR + BB) as float)/ PA as 'OBP'
    ,cast((single + 2*double + 3*triple + 4*HR) as float) / AB as 'SLG'
    ,BB
    ,K
    ,(single + double + triple + HR) as 'H'
    ,single as '1B'
    ,double as '2B'
    ,triple as '3B'
    ,DATE('now') as 'Updated'
FROM TOTALS),

woba as (
select
 batter
 ,avg(woba_value) as 'woba'
from pitch_data
where woba_value is not null
group by batter
),

xwobacon as (
select
 batter
 ,avg(estimated_woba_using_speedangle) as 'xwobacon'
from pitch_data
where estimated_woba_using_speedangle is not null
group by batter
)



select
  standard.batter
  ,Name
  ,Team
  ,POS
  ,PA
  ,OBP + SLG as 'OPS'
  ,cast(BB as float) / cast(PA as float) as 'BB_PCT'
  ,cast(K as float) / cast(PA as float) as 'K_PCT'
  ,woba.woba
  ,xwobacon.xwobacon
FROM standard
left join woba on standard.batter = woba.batter
left join xwobacon on standard.batter = xwobacon.batter
order by PA desc