
import sqlite3
from baseball_scraper import statcast
from datetime import date
from datetime import timedelta

today = str(date.today())
yesterday = str(date.today() - timedelta(days = 1))

new_data = statcast(start_dt = '2022-04-07', end_dt = today)

connection = sqlite3.connect('C:\Programming\Projects\mlbproject\mlb.db')
cursor = connection.cursor()


new_data.to_sql("pitch_data", connection, if_exists="replace")

remove_duplicates_pitch_data_query = '''DROP TABLE IF EXISTS batter_standard;'''

drop_batter_standard_query = '''DROP TABLE IF EXISTS batter_standard;'''

drop_standings_query =  '''DROP TABLE IF EXISTS standings;'''

drop_pitcher_standard_query = '''DROP TABLE IF EXISTS pitcher_standard'''

drop_batter_advanced_query = '''DROP TABLE IF EXISTS batter_advanced'''

drop_run_differential_query = '''DROP TABLE IF EXISTS run_differential'''

drop_team_batting_query = '''DROP TABLE IF EXISTS team_batting'''

drop_team_pitching_query = ''' DROP TABLE IF EXISTS team_pitching'''

drop_team_batting_ranks_query = '''DROP TABLE IF EXISTS team_batting_ranks'''

drop_team_pitching_ranks_query = '''DROP TABLE IF EXISTS team_pitching_ranks'''

drop_team_woba_batting_rolling_query = ''' DROP TABLE IF EXISTS team_woba_batting_rolling'''

drop_team_woba_pitching_rolling_query = ''' DROP TABLE IF EXISTS team_woba_pitching_rolling'''

drop_batter_woba_rolling_query = ''' DROP TABLE IF EXISTS batter_woba_rolling'''

drop_pitcher_woba_rolling_query = ''' DROP TABLE IF EXISTS pitcher_woba_rolling'''

create_batter_standard_query = '''CREATE TABLE batter_standard as 

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
    ,SUM(CASE WHEN events = "walk" THEN 1 ELSE 0 END) AS "BB"
    ,SUM(CASE WHEN events = "hit_by_pitch" THEN 1 ELSE 0 END) AS "HBP"
    ,SUM(CASE WHEN events = "strikeout" THEN 1 ELSE 0 END) AS "K"
FROM (select distinct * from pitch_data) as pitch_data
LEFT JOIN player_ids on pitch_data.batter = player_ids.MLBID
GROUP BY batter, MLBNAME)

SELECT
    batter
    ,Name
    ,Team
    ,POS
    ,PA
    ,AB
    ,HR
    ,ROUND(cast((single + double + triple + HR)as float) / cast(AB as float),3) as 'AVG'
    ,ROUND(cast((single + double + triple + HR + BB + HBP) as float)/ PA,3) as 'OBP'
    ,ROUND(cast((single + 2*double + 3*triple + 4*HR) as float) / AB,3) as 'SLG'
    ,(single + double + triple + HR) as 'H'
    ,single as '1B'
    ,double as '2B'
    ,triple as '3B'
    ,DATE('now') as 'Updated'
FROM TOTALS
;'''

create_standings_query = '''CREATE TABLE standings as 

-- SQLite
WITH game_results AS (
SELECT 
 game_date
,home_team
,away_team
,max(post_home_score) as 'home_final'
,max(post_away_score) as 'away_final'
,case when max(post_home_score) > max(post_away_score) then 1 else 0 end as 'home_win'
,case when max(post_home_score) < max(post_away_score) then 1 else 0 end as 'home_loss'
,case when max(post_away_score) > max(post_home_score) then 1 else 0 end as 'away_win'
,case when max(post_away_score) < max(post_home_score) then 1 else 0 end as 'away_loss'
,max(post_home_score) - max(post_away_score)  as 'home_differential'
,max(post_away_score) - max(post_home_score) as 'away_differential'
FROM pitch_data
group by game_pk,game_date, home_team, away_team
order by game_date desc)

SELECT 
 Team
,SUM(wins) as 'wins'
,SUM(losses) as 'losses'
,ROUND(cast(SUM(wins)as float) / (cast(SUM(wins)as float) + cast(SUM(losses)as float)),3) as 'win_pct'
,SUM(run_differential) as 'run_differential'
,CASE WHEN Team in ('NYY','TOR','TB','BOS','BAL') THEN 'AL East'
      WHEN Team in ('MIN','CLE','CWS','DET','KC') THEN 'AL Central'
      WHEN Team in ('HOU','SEA','TEX','LAA','OAK') THEN 'AL West'
      WHEN Team in ('NYM','PHI','ATL','MIA','WSH') THEN 'NL East'
      WHEN Team in ('MIL','CHC','STL','PIT','CIN') THEN 'NL Central'
      WHEN Team in ('LAD','SD','ARI','COL','SF') THEN 'NL West' END AS 'Division'
FROM (
    SELECT 
    home_team as 'Team'
    ,SUM(home_win) as 'wins'
    ,SUM(home_loss) as 'losses'
    ,SUM(home_differential) as 'run_differential'
    FROM game_results
    GROUP BY home_team

    UNION 

    SELECT 
    away_team as 'Team'
    ,SUM(away_win) as 'wins'
    ,SUM(away_loss) as 'losses'
    ,SUM(away_differential) as 'run_differential'
    FROM game_results
    GROUP BY away_team )
GROUP BY Team
order by wins desc

'''

create_pitcher_standard_query = ''' CREATE TABLE pitcher_standard as

-- SQLite
WITH events AS (
SELECT 
 pitcher
 ,SUM(CASE WHEN events is not null then 1 else 0 end) as 'PA'
 ,SUM(CASE WHEN events in ('single', 'double', 'triple', 'home_run') then 1 else 0 end) as 'H'
 ,SUM(CASE WHEN events = 'strikeout' then 1 else 0 end) as 'K'
 ,SUM(CASE WHEN events in ('walk') then 1 else 0 end) as 'BB'
 ,SUM(CASE WHEN events = 'home_run' then 1 else 0 end) as 'HR'
 ,SUM(CASE WHEN events = 'field_out' then 1
           WHEN events = 'sac_bunt' then 1
           WHEN events = 'strikeout' then 1
           WHEN events = 'pickoff_2b' then 1
           WHEN events = 'pickoff_3b' then 1
           WHEN events = 'sac_fly_double_play' then 2
           WHEN events = 'pickoff_caught_stealing_home' then 1
           WHEN events = 'caught_stealing_home' then 1
           WHEN events = 'caught_stealing_3b' then 1
           WHEN events = 'pickoff_caught_stealing_3b' then 1
           WHEN events = 'fielders_choice_out' then 1
           WHEN events = 'other_out' then 1
           WHEN events = 'caught_stealing_2b' then 1
           WHEN events = 'pickoff_1b' then 1
           WHEN events = 'force_out' then 1
           WHEN events = 'fielders_choice' then 1
           WHEN events = 'sac_fly' then 1
           WHEN events = 'double_play' then 2
           WHEN events = 'strikeout_double_play' then 2
           WHEN events = 'grounded_into_double_play' then 2
           WHEN events = 'triple_play' then 3
           ELSE 0 end) as 'Outs'
FROM pitch_data
GROUP BY pitcher),

RUNS AS (
SELECT
 pitcher
 ,player_name
 ,SUM(RA) as 'RA'
 FROM (
SELECT
 pitcher
 ,player_name
 ,MAX(post_bat_score) - MIN(bat_score) as 'RA'
FROM pitch_data
GROUP BY game_pk, pitcher, player_name)
GROUP BY pitcher)

SELECT
 RUNS.pitcher
 ,player_ids.TEAM
 ,player_name
 ,events.Outs / 3 as 'IP'
 ,events.H 
 ,RA as 'R'
 ,events.HR
 ,events.BB
 ,events.K
 ,(RA / (events.Outs / 3))*9 as 'RA9'
 ,ROUND((cast(events.BB as float) / cast(events.PA as float))*100,2) as 'BB_pct'
 ,ROUND((cast(events.K as float) / cast(events.PA as float))*100,2) as 'K_pct'
FROM RUNS
LEFT JOIN events on RUNS.pitcher = events.pitcher
LEFT JOIN player_ids on RUNS.pitcher = player_ids.MLBID
order by K desc

'''

create_batter_advanced_query = '''CREATE TABLE batter_advanced AS 

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
    ,SUM(CASE WHEN events = "walk" THEN 1 ELSE 0 END) AS "BB"
    ,SUM(CASE WHEN events = "hit_by_pitch" THEN 1 ELSE 0 END) AS "HBP"
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
    ,ROUND(cast((single + double + triple + HR)as float) / cast(AB as float),3) as 'AVG'
    ,ROUND(cast((single + double + triple + HR + BB + HBP) as float)/ PA,3) as 'OBP'
    ,ROUND(cast((single + 2*double + 3*triple + 4*HR) as float) / AB,3) as 'SLG'
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
 ,ROUND(avg(woba_value),3) as 'woba'
from pitch_data
where woba_value is not null
group by batter
),

xwobacon as (
select
 batter
 ,ROUND(avg(estimated_woba_using_speedangle),3) as 'xwobacon'
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
  ,ROUND(OBP + SLG,3) as 'OPS'
  ,ROUND((cast(BB as float) / cast(PA as float))*100,2) as 'BB_PCT'
  ,ROUND((cast(K as float) / cast(PA as float))*100,2) as 'K_PCT'
  ,woba.woba
  ,xwobacon.xwobacon
FROM standard
left join woba on standard.batter = woba.batter
left join xwobacon on standard.batter = xwobacon.batter
order by PA desc

'''

create_run_differential_query = '''CREATE TABLE run_differential AS 

-- SQLite
WITH game_results AS (
SELECT 
 game_date
,home_team
,away_team
,max(post_home_score) as 'home_final'
,max(post_away_score) as 'away_final'
,case when max(post_home_score) > max(post_away_score) then 1 else 0 end as 'home_win'
,case when max(post_home_score) < max(post_away_score) then 1 else 0 end as 'home_loss'
,case when max(post_away_score) > max(post_home_score) then 1 else 0 end as 'away_win'
,case when max(post_away_score) < max(post_home_score) then 1 else 0 end as 'away_loss'
,max(post_home_score) - max(post_away_score)  as 'home_differential'
,max(post_away_score) - max(post_home_score) as 'away_differential'
FROM pitch_data
group by game_pk,game_date, home_team, away_team
order by game_date desc),

differential as (
SELECT
home_team as 'Team'
,game_date
,home_differential as 'differential'
from game_results


UNION

SELECT
away_team as 'Team'
,game_date
,away_differential as  'differential'
from game_results)

SELECT
Team
,row_number () over (partition by Team order by game_date) as game_number
,differential as 'game_differential'
,SUM(differential) over (partition by Team order by game_date) as 'Total'
from differential
order by Team, row_number () over (partition by Team order by game_date) 

'''
create_team_batting_query = ''' CREATE TABLE team_batting as 
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
    ,cast(SUM(CASE WHEN events = "walk" THEN 1 ELSE 0 END) as float) AS "BB"
    ,cast(SUM(CASE WHEN events = "hit_by_pitch" THEN 1 ELSE 0 END) as float) AS "HBP"
    ,cast(SUM(CASE WHEN events = "strikeout" THEN 1 ELSE 0 END) as float) AS "K"
    ,sum(woba_value) as "woba"
,max(post_home_score) as 'runs_scored'
FROM pitch_data
WHERE inning_topbot = 'Bot'
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
    ,cast(SUM(CASE WHEN events = "walk" THEN 1 ELSE 0 END) as float) AS "BB"
    ,cast(SUM(CASE WHEN events = "hit_by_pitch" THEN 1 ELSE 0 END) as float) AS "HBP"
    ,cast(SUM(CASE WHEN events = "strikeout" THEN 1 ELSE 0 END) as float) AS "K"
    ,sum(woba_value) as "woba"
,max(post_away_score) as 'runs_scored'
FROM pitch_data
WHERE inning_topbot = 'Top'
group by game_pk, game_date, away_team)

SELECT
Team
,ROUND(sum(runs_scored) / count(game_pk),2) as 'R/G'
,sum(HR) as 'HR'
,ROUND(SUM(H) / SUM(AB),3) AS 'BA'
,ROUND((SUM(H) + SUM(BB) + SUM(HBP)) / SUM(PA),3) AS 'OBP'
,ROUND((SUM(single) + SUM(double*2) + SUM(triple*3) + SUM(HR*4)) / SUM(AB),3) AS 'SLG'
,ROUND(((SUM(H)+SUM(BB)) / SUM(PA)) + ((SUM(single) + SUM(double*2) + SUM(triple*3) + SUM(HR*4)) / SUM(AB)),3) AS 'OPS'
,ROUND((SUM(BB) / SUM(PA))*100,2) AS 'BB_Rate'
,ROUND((SUM(K) / SUM(PA))*100,2) AS 'K_Rate'
,ROUND(sum(woba) / sum(PA),3) AS 'woba'
FROM game_totals
GROUP BY Team
ORDER BY sum(runs_scored) / count(game_pk) desc

'''

create_team_pitching_query = '''CREATE TABLE team_pitching as 
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
    ,cast(SUM(CASE WHEN events IN("walk") THEN 1 ELSE 0 END) as float) AS "BB"
    ,cast(SUM(CASE WHEN events = "strikeout" THEN 1 ELSE 0 END) as float) AS "K"
    ,sum(woba_value) as "woba"
,max(post_home_score) as 'runs_allowed'
FROM pitch_data
WHERE inning_topbot = 'Bot'
group by game_pk, game_date, away_team)

SELECT
Team
,ROUND(sum(runs_allowed) / count(game_pk),2) as 'RA/G'
,sum(HR) as 'HR'
,ROUND(SUM(H) / SUM(AB),3) AS 'BA'
,ROUND((SUM(H)+SUM(BB)) / SUM(PA),3) AS 'OBP'
,ROUND((SUM(single) + SUM(double*2) + SUM(triple*3) + SUM(HR*4)) / SUM(AB),3) AS 'SLG'
,ROUND(((SUM(H)+SUM(BB)) / SUM(PA)) + ((SUM(single) + SUM(double*2) + SUM(triple*3) + SUM(HR*4)) / SUM(AB)),3) AS 'OPS'
,ROUND((SUM(BB) / SUM(PA))*100,2) AS 'BB_Rate'
,ROUND((SUM(K) / SUM(PA))*100,2) AS 'K_Rate'
,ROUND(sum(woba) / sum(PA),3) AS 'woba'
FROM game_totals
GROUP BY Team
ORDER BY sum(runs_allowed) / count(game_pk) asc

'''

create_team_batting_ranks_query = '''CREATE TABLE team_batting_ranks AS
SELECT
Team
,rank() over(order by team_batting.[R/G] desc) as 'R/G'
,rank() over(order by HR desc) as 'HR'
,rank() over(order by BA desc) as 'BA'
,rank() over(order by OBP desc) as 'OBP'
,rank() over(order by SLG desc) as 'SLG'
,rank() over(order by OPS desc) as 'OPS'
,rank() over(order by BB_Rate desc) as 'BB'
,rank() over(order by K_Rate asc) as 'K'
,rank() over(order by woba desc) as 'woba'
FROM team_batting

'''

create_team_pitching_ranks_query = '''CREATE TABLE team_pitching_ranks AS
SELECT
Team
,rank() over(order by team_pitching.[RA/G] asc) as 'RA/G'
,rank() over(order by HR asc) as 'HR'
,rank() over(order by BA asc) as 'BA'
,rank() over(order by OBP asc) as 'OBP'
,rank() over(order by SLG asc) as 'SLG'
,rank() over(order by OPS asc) as 'OPS'
,rank() over(order by BB_Rate asc) as 'BB'
,rank() over(order by K_Rate desc) as 'K'
,rank() over(order by woba asc) as 'woba'
FROM team_pitching

'''

create_team_woba_batting_rolling_query = '''CREATE TABLE team_woba_batting_rolling AS 

with woba_game as (
SELECT 
 game_date
,game_pk
,home_team as 'Team'
,round(avg(woba_value),3) as "woba"
FROM pitch_data
WHERE inning_topbot = 'Bot'
group by game_pk, game_date, home_team

UNION

SELECT 
 game_date
,game_pk
,away_team as 'Team'
,round(avg(woba_value),3) as "woba"
FROM pitch_data
WHERE inning_topbot = 'Top'
group by game_pk, game_date, away_team)

SELECT
Team
,game_date
,row_number () over (partition by Team order by game_date) as game_number
,ROUND(woba,3) as "woba"
,avg(woba) over(partition by Team order by game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS 'rolling_average_woba'
FROM woba_game

'''

create_team_woba_pitching_rolling_query = '''CREATE TABLE team_woba_pitching_rolling AS 

with woba_game as (
SELECT 
 game_date
,game_pk
,home_team as 'Team'
,ROUND(avg(woba_value),3) as "woba"
FROM pitch_data
WHERE inning_topbot = 'Top'
group by game_pk, game_date, home_team

UNION

SELECT 
 game_date
,game_pk
,away_team as 'Team'
,ROUND(avg(woba_value),3) as "woba"
FROM pitch_data
WHERE inning_topbot = 'Bot'
group by game_pk, game_date, away_team)

SELECT
Team
,game_date
,row_number () over (partition by Team order by game_date) as game_number
,woba
,avg(woba) over(partition by Team order by game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS 'rolling_average_woba'
FROM woba_game

'''

create_batter_woba_rolling_query = ''' CREATE TABLE batter_woba_rolling AS 


with woba_game as (
SELECT 
batter
,level_0
,game_date
,round(avg(woba_value),3) as "woba"
FROM pitch_data
WHERE events is not null
group by game_pk,game_date, batter, level_0),

rolling as (
SELECT
Team
,playername
,batter
,level_0
,row_number () over (partition by batter order by level_0 desc) as PA
,woba
,avg(woba) over(partition by batter order by level_0 desc ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS 'rolling_average_woba'
FROM woba_game
left join player_ids on woba_game.batter = player_ids.MLBID)

select * from rolling
where PA > 49 


'''

create_pitcher_woba_rolling_query = ''' CREATE TABLE pitcher_woba_rolling AS

with woba_game as (
SELECT 
pitcher
,level_0
,game_date
,round(avg(woba_value),3) as "woba"
FROM pitch_data
WHERE events is not null
group by game_pk,game_date, pitcher, level_0),

rolling as (
SELECT
Team
,playername
,pitcher
,level_0
,row_number () over (partition by pitcher order by level_0 desc) as PA
,woba
,avg(woba) over(partition by pitcher order by level_0 desc ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS 'rolling_average_woba'
FROM woba_game
left join player_ids on woba_game.pitcher = player_ids.MLBID)

select * from rolling
where PA > 49


'''


cursor.execute(drop_batter_standard_query)
cursor.execute(drop_standings_query)
cursor.execute(drop_pitcher_standard_query)
cursor.execute(drop_batter_advanced_query)
cursor.execute(drop_run_differential_query)
cursor.execute(drop_team_batting_query)
cursor.execute(drop_team_pitching_query)
cursor.execute(drop_team_batting_ranks_query)
cursor.execute(drop_team_pitching_ranks_query)
cursor.execute(drop_team_woba_batting_rolling_query)
cursor.execute(drop_team_woba_pitching_rolling_query)
cursor.execute(drop_batter_woba_rolling_query)
cursor.execute(drop_pitcher_woba_rolling_query)
cursor.execute(create_batter_standard_query)
cursor.execute(create_standings_query)
cursor.execute(create_pitcher_standard_query)
cursor.execute(create_batter_advanced_query)
cursor.execute(create_run_differential_query)
cursor.execute(create_team_batting_query)
cursor.execute(create_team_pitching_query)
cursor.execute(create_team_batting_ranks_query)
cursor.execute(create_team_pitching_ranks_query)
cursor.execute(create_team_woba_batting_rolling_query)
cursor.execute(create_team_woba_pitching_rolling_query)
cursor.execute(create_batter_woba_rolling_query)
cursor.execute(create_pitcher_woba_rolling_query)

connection.close()
print('Script finished')