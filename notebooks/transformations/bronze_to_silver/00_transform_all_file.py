# Databricks notebook source
# MAGIC %md
# MAGIC ## The files of circuits, races, constructors and drivers will have all the data.
# MAGIC * So there is no need of implimentation of incremental load for these files
# MAGIC * However in other left over files, we will have incremental data i.e.
# MAGIC 1. 2021-03-21 will contain all the data, from start till 2021-03-21
# MAGIC 2. other files i.e. 2021-03-28 and 2021-04-18 will have the data for that particular date.

# COMMAND ----------

v_result = dbutils.notebook.run("01_transform_circuits", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("02_transform_races", 0,{"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("03_transform_constructors", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("04_transform_drivers", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("05_transform_results", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("06_transform_pit_stops", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("07_transform_lap_times", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("08_transform_qualifying", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result