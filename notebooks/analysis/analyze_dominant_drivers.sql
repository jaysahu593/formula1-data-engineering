-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1. Let's calculate the total points for a driver.
-- MAGIC 2. But over the time, there will be variation for the no. of races the driver has driven.
-- MAGIC 3. So let's calculate the AVG point a driver has scored in total.

-- COMMAND ----------

SELECT driver_name,
        COUNT(*) AS total_races,
        -- ROUND(SUM(calculated_points)/COUNT(*), 2) AS avg_points,
        ROUND(AVG(calculated_points), 2) AS avg_points, 
        SUM(calculated_points) AS total_points 
  FROM f1_catalog.f1_presentation.calculated_race_results
  GROUP BY driver_name
  HAVING count(*) >= 50
  ORDER BY avg_points DESC
  LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Lets check the points for a particular decade

-- COMMAND ----------

SELECT driver_name,
        COUNT(*) AS total_races,
        -- ROUND(SUM(calculated_points)/COUNT(*), 2) AS avg_points,
        ROUND(AVG(calculated_points), 2) AS avg_points, 
        SUM(calculated_points) AS total_points 
  FROM f1_catalog.f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING count(*) >= 50
ORDER BY avg_points DESC
LIMIT 10;

-- COMMAND ----------

