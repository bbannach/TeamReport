
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

