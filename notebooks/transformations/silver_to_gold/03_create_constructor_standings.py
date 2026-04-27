# Databricks notebook source
dbutils.widgets.text("p_file_date", '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../../utils/config"

# COMMAND ----------

# MAGIC %run "../../utils/common"

# COMMAND ----------

# MAGIC %run "../../utils/auth/adls_service_principal_auth"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{gold_folder_path}/race_results")\
                            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# race_results_list = race_results_df\
#                             .select('race_year')\
#                             .distinct()\
#                             .collect()

# race_year_list = []

# for race_year in race_results_list:
#     race_year_list.append(race_year.race_year)
# race_year_list

## Created a common fuction for this

race_year_list = df_column_to_list(race_results_df, 'race_year')


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

race_results_df = spark.read.parquet(f"{gold_folder_path}/race_results")\
                            .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructor_standing_df = race_results_df.groupBy("race_year", "team")\
                                    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins") )

# COMMAND ----------

from pyspark.sql.window import *

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_df = constructor_standing_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

# final_df.write.mode("append").parquet(f"{gold_folder_path}/constructor_standings")
# final_df.write.mode("append").parquet(f"{presentation_folder_path}/constructor_standings")

# Creating managed tables

overwrite_partition(final_df, "f1_catalog", "f1_presentation", "constructor_standings", "race_year")

# Creating ext tables
overwrite_partition_ext(final_df, "f1_catalog_ext", "f1_presentation_ext", "constructor_standings", "race_year")

# final_df.write.mode("overwrite").format("delta").saveAsTable("f1_catalog.f1_presentation.constructor_standings")

# COMMAND ----------

