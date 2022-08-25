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




