-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Finding Dominating Teams

-- COMMAND ----------

SELECT team_name,
        COUNT(*) AS total_races,
        -- ROUND(SUM(calculated_points)/COUNT(*), 2) AS avg_points,
        ROUND(AVG(calculated_points), 2) AS avg_points, 
        SUM(calculated_points) AS total_points 
  FROM f1_catalog.f1_presentation.calculated_race_results
  GROUP BY team_name
  HAVING count(*) >= 100
  ORDER BY avg_points DESC
  LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Let's look for the last 10 years

-- COMMAND ----------

SELECT team_name,
        COUNT(*) AS total_races,
        -- ROUND(SUM(calculated_points)/COUNT(*), 2) AS avg_points,
        ROUND(AVG(calculated_points), 2) AS avg_points, 
        SUM(calculated_points) AS total_points 
  FROM f1_catalog.f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING count(*) >= 100
ORDER BY avg_points DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * similarly before that decade

-- COMMAND ----------

SELECT team_name,
        COUNT(*) AS total_races,
        -- ROUND(SUM(calculated_points)/COUNT(*), 2) AS avg_points,
        ROUND(AVG(calculated_points), 2) AS avg_points, 
        SUM(calculated_points) AS total_points 
  FROM f1_catalog.f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2001 AND 2011
GROUP BY team_name
HAVING count(*) >= 100
ORDER BY avg_points DESC
LIMIT 10;

-- COMMAND ----------

