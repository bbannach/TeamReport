WITH PA AS (
    SELECT
        pitch_data.batter
        ,game_year
        ,SUM(CASE WHEN events ISNULL THEN 0 ELSE 1 END) AS 'PA'
        ,TPA.'Total_PA'
    FROM pitch_data 
    LEFT JOIN (SELECT batter ,SUM(CASE WHEN events ISNULL THEN 0 ELSE 1 END) AS 'Total_PA'
        FROM pitch_data
        WHERE game_year < 2021
        GROUP BY batter) AS TPA ON pitch_data.batter = TPA.batter
    GROUP BY pitch_data.batter, game_year
    HAVING pitch_data.batter = '545361'),

con_PA AS (
    SELECT
        batter
        ,game_year
        ,COUNT(estimated_woba_using_speedangle) AS 'con_PA'
    FROM pitch_data 
    WHERE estimated_woba_using_speedangle IS NOT NULL
    GROUP BY batter, game_year),

wOBA AS (
    SELECT
        PD.batter
        ,PD.game_year
        ,CASE WHEN pd.game_year = 2020 THEN 5
              WHEN pd.game_year = 2019 THEN 3
              WHEN pd.game_year = 2018 THEN 1
              ELSE 0 END AS 'Season_Weight'
        ,PA.PA
        ,SUM(woba_value)/PA.PA AS 'wOBA'
        ,(SUM(estimated_woba_using_speedangle) + SUM(CASE WHEN events = 'walk' OR events = 'strikeout' THEN woba_value ELSE 0 END)) / PA.PA AS 'xWOBA'
        ,SUM(estimated_woba_using_speedangle) / con_PA.con_PA as 'xWOBA_con'
        ,CAST(SUM(CASE WHEN events = 'walk' THEN 1 ELSE 0 END) AS FLOAT) / PA.PA AS 'BB_pct'
        ,CAST(SUM(CASE WHEN events = 'strikeout' THEN 1 ELSE 0 END) AS FLOAT) / PA.PA AS 'K_pct'
    FROM pitch_data AS PD
    LEFT JOIN PA ON PD.batter = PA.batter AND PD.game_year = PA.game_year
    LEFT JOIN con_PA on PD.batter = con_PA.batter and PD.game_year = con_PA.game_year
    GROUP BY PD.batter, PD.game_year
    HAVING PD.batter = '545361'
    ORDER BY PD.game_year DESC)

SELECT
    batter
    ,game_year
    ,wOBA
    ,SUM(xWOBA_con*Season_Weight*PA)/SUM(Season_Weight*PA) AS 'weighted_xWOBA_con'
    ,SUM('BB_pct'*Season_Weight*PA)/SUM(Season_Weight*PA) AS 'weighted_BB_pct'
    ,SUM('K_pct'*Season_Weight*PA)/SUM(Season_Weight*PA) AS 'weighted_K_pct'
FROM wOBA
GROUP BY batter