# Databricks notebook source
# MAGIC %md
# MAGIC ### RUN with caution, risk of data duplication in silver layer

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

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
# MAGIC
# MAGIC ###Step 1 - Read the JSON file

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

results_schema = StructType(fields=[
                                    StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", StringType(), True),
                                    StructField("statusId", StringType(), True)
                                    ])

# COMMAND ----------

results_df = spark.read.schema(results_schema)\
                        .json(f"{bronze_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Step 2 - Rename, Drop and create the columns

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id") \
                                .withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("constructorId", "constructor_id") \
                                .withColumnRenamed("positionText", "position_text") \
                                .withColumnRenamed("positionOrder", "position_order") \
                                .withColumnRenamed("fastestLap", "fastest_lap") \
                                .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
                                .withColumn("data_source", lit(v_data_source))\
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_final_df = add_ingestion_date(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Droping duplicates

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4 - Write the data with partition

# COMMAND ----------

# MAGIC %md
# MAGIC * In databrick we don't have direct option for incremental load untile we are using delta format.
# MAGIC * So we needs to add the logic of first delete the existing record and then update new one

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1, for incremental load
# MAGIC
# MAGIC * This below loop will verify if the data already exists, if yes, then it will delete the existing data in table and then ingest the data, to overcome duplicates. If not it will be skipped.

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if spark.catalog.tableExists("f1_catalog.f1_processed.results"):
#            spark.sql(f"DELETE FROM f1_catalog.f1_processed.results WHERE race_id = {race_id_list.race_id}")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").parquet(f"{silver_folder_path}results")

# creating managed tables   

# results_final_df.write.mode("append").partitionBy("race_id").format("delta").saveAsTable("f1_catalog.f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2, for incremental load
# MAGIC * We will be using INSERT INTO funcitonality to do the incremental load. This will append the new data in table. 
# MAGIC * Here we cannot specify the order of column here but spark expects the last inserted column in the list to be partitioned col.
# MAGIC
# MAGIC * So first use below statement to write the data first i.e. doing a full load or cutover.
# MAGIC => results_final_df.write.mode("append").partitionBy("race_id").format("delta").saveAsTable("f1_catalog.f1_processed.results")
# MAGIC
# MAGIC * Then use below query for incremental load
# MAGIC => results_final_df.write.mode("append").partitionBy("race_id").insertInto("f1_catalog.f1_processed.results")
# MAGIC
# MAGIC * Here we also needs to make sure that our partition column is the last in schema, to make INSERT INTO work properly .
# MAGIC
# MAGIC -> So we need to make these changes in other tables as well, so we created 2 functions to make this process more easy.
# MAGIC
# MAGIC 1. **re_arrange_partition_columne()**
# MAGIC * This will rearrange the partition column in the last to make sure, INSERT INTO works propertly
# MAGIC
# MAGIC 2. **overwrite_partition()**
# MAGIC * This function will, take the final_df, catalog_name, db_name, table_name and partition_column.
# MAGIC * This will impliment the logic of first changing the overwrite mode from static to dynamic.
# MAGIC * Then checking if full load/cut over is needed or incremental load, and will perform the task accordingly.

# COMMAND ----------

# Copied these function to common_function notebook, so we don't need them here anymore

# def re_arrange_partition_column(input_df, partition_column):
#     column_list = []
#     for column_name in input_df.schema.names:
#         if column_name != partition_column:
#             column_list.append(column_name)
#     column_list.append(partition_column)
#     output_df = input_df.select(column_list)
#     return output_df

# COMMAND ----------

output_df = re_arrange_partition_column(results_deduped_df, "race_id")

# COMMAND ----------

# Copied these function to common_function notebook, so we don't need them here anymore

# def overwrite_partition(input_df, catalog_name, db_name, table_name, partition_column):
#     output_df = re_arrange_partition_column(input_df, partition_column)
 
#     spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
#     if spark.catalog.tableExists(f"{catalog_name}.{db_name}.{table_name}"):
#         output_df.write.mode("overwrite").insertInto(f"{catalog_name}.{db_name}.{table_name}")
#     else:
#         output_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{catalog_name}.{db_name}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC **This is set the overwrite mode to dynamic and will only overwrite the full data when needed otherwise will only add the new data to table.**

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# CREATING MANAGED TABLES
overwrite_partition(results_deduped_df, 'f1_catalog', 'f1_processed', 'results', 'race_id')

# CREATING EXTERNAL TABLES
overwrite_partition_ext(results_deduped_df, 'f1_catalog_ext', 'f1_processed_ext', 'results', 'race_id')

# CREATING PARQUET FILES
# Use below query with caution as it could overwrite the data in external location.
# results_deduped_df.write.mode("append").partitionBy("race_id").parquet(f"{silver_folder_path}results")

# results_deduped_df.write.mode("append").partitionBy("race_id").parquet(f"{processed_folder_path}results")

# COMMAND ----------

dbutils.notebook.exit("Success")