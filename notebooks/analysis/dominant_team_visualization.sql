-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP view v_dominant_teams
AS
SELECT team_name,
        COUNT(*) AS total_races,
        ROUND(AVG(calculated_points), 2) AS avg_points, 
        SUM(calculated_points) AS total_points,
        RANK() OVER( ORDER BY ROUND(AVG(calculated_points),2) DESC) AS _rank
  FROM f1_catalog.f1_presentation.calculated_race_results
  GROUP BY team_name
  HAVING count(*) >= 100
  ORDER BY avg_points DESC

-- COMMAND ----------

-- Line Chart

SELECT race_year,
        team_name,
        COUNT(*) AS total_races,
        ROUND(AVG(calculated_points), 2) AS avg_points, 
        SUM(calculated_points) AS total_points
  FROM f1_catalog.f1_presentation.calculated_race_results
  WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <=5)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

-- Area chat
SELECT race_year,
        team_name,
        COUNT(*) AS total_races,
        ROUND(AVG(calculated_points), 2) AS avg_points, 
        SUM(calculated_points) AS total_points
  FROM f1_catalog.f1_presentation.calculated_race_results
  WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <=5)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

