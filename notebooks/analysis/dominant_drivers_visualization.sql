-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Creating a rank function to get the rank of the drivers

-- COMMAND ----------

CREATE OR REPLACE TEMP view v_dominant_driver
AS
SELECT driver_name,
        COUNT(*) AS total_races,
        ROUND(AVG(calculated_points), 2) AS avg_points, 
        SUM(calculated_points) AS total_points,
        RANK() OVER( ORDER BY ROUND(AVG(calculated_points),2) DESC) AS driver_rank
  FROM f1_catalog.f1_presentation.calculated_race_results
  GROUP BY driver_name
  HAVING count(*) >= 50
  ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * So as shown in below query result, we don't want all the driver's result.
-- MAGIC * Rather we can just have top 3 or 5 for that year.
-- MAGIC * In order to do it, we can either use CTE or we can create a view from table and using IN function

-- COMMAND ----------

-- SELECT race_year,
--         driver_name,
--         COUNT(*) AS total_races,
--         ROUND(AVG(calculated_points), 2) AS avg_points, 
--         SUM(calculated_points) AS total_points
--   FROM f1_catalog.f1_presentation.calculated_race_results
-- GROUP BY race_year, driver_name
-- ORDER BY race_year, avg_points DESC
-- LIMIT 10;

-- for the limited drivers.

SELECT race_year,
        driver_name,
        COUNT(*) AS total_races,
        ROUND(AVG(calculated_points), 2) AS avg_points, 
        SUM(calculated_points) AS total_points
  FROM f1_catalog.f1_presentation.calculated_race_results
  WHERE driver_name IN (SELECT driver_name FROM v_dominant_driver WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

-- total races and total points for the drivers over the year

SELECT race_year,
        driver_name,
        COUNT(*) AS total_races,
        ROUND(AVG(calculated_points), 2) AS avg_points, 
        SUM(calculated_points) AS total_points
  FROM f1_catalog.f1_presentation.calculated_race_results
  WHERE driver_name IN (SELECT driver_name FROM v_dominant_driver WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT race_year,
        driver_name,
        COUNT(*) AS total_races,
        ROUND(AVG(calculated_points), 2) AS avg_points, 
        SUM(calculated_points) AS total_points
  FROM f1_catalog.f1_presentation.calculated_race_results
  WHERE driver_name IN (SELECT driver_name FROM v_dominant_driver WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

