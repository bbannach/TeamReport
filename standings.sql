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
,cast(SUM(wins)as float) / (cast(SUM(wins)as float) + cast(SUM(losses)as float)) as 'win_pct'
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

