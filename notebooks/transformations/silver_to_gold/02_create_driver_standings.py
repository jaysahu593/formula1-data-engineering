# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../../utils/config"

# COMMAND ----------

# MAGIC %run "../../utils/common"

# COMMAND ----------

# MAGIC %run "../../utils/auth/adls_service_principal_auth"

# COMMAND ----------

# MAGIC %md
# MAGIC **Find race year for which the data needs to be reprocessed**

# COMMAND ----------

race_results_list = spark.read.parquet(f"{gold_folder_path}/race_results")\
                                .filter(f"file_date = '{v_file_date}'")\
                                .select("race_year")\
                                .distinct()\
                                .collect()

# COMMAND ----------

# race_results_list

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------

race_results_df = spark.read.parquet(f"{gold_folder_path}/race_results")\
                            .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

driver_standing_df = race_results_df.groupBy("race_year", "driver_name" ,"driver_nationality")\
                                    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins") )

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_df = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# final_df.write.mode("append").parquet(f"{presentation_folder_path}/driver_standings")

# final_df.write.mode("append").parquet(f"{gold_folder_path}/driver_standings")

# Creating managed tables
overwrite_partition(final_df, "f1_catalog", "f1_presentation", "driver_standings", "race_year")

# Creating external tables
overwrite_partition_ext(final_df, "f1_catalog_ext", "f1_presentation_ext", "driver_standings", "race_year")

# final_df.write.mode("overwrite").format('delta').saveAsTable("f1_catalog.f1_presentation.driver_standings")

# COMMAND ----------

