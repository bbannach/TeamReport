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
