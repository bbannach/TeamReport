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
 ,cast(events.BB as float) / cast(events.PA as float) as 'BB%'
 ,cast(events.K as float) / cast(events.PA as float) as 'K%'
FROM RUNS
LEFT JOIN events on RUNS.pitcher = events.pitcher
LEFT JOIN player_ids on RUNS.pitcher = player_ids.MLBID
order by K desc



