
with woba_game as (
SELECT 
 game_date
,game_pk
,home_team as 'Team'
,avg(woba_value) as "woba"
FROM pitch_data
WHERE inning_topbot = 'Top'
group by game_pk, game_date, home_team

UNION

SELECT 
 game_date
,game_pk
,away_team as 'Team'
,avg(woba_value) as "woba"
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

