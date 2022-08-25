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