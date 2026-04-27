# Databricks notebook source
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
# MAGIC ### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

races_schema = StructType(fields=[StructField("race_id", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True),
                                  ])

# COMMAND ----------

races_df = spark.read\
                    .option("header", True)\
                    .schema(races_schema)\
                    .csv(f"{bronze_folder_path}/{v_file_date}/races.csv")

# time col had \N value in place of null values so using below query. So first we converted \n values to null and then filled null values with defautl values (00:00:00)

races_df = races_df.withColumn("time", when(col('time') == '\\N', None).otherwise(col('time')))
races_df = races_df.fillna({"time": "00:00:00"})

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

races_with_timestamp_df = races_df\
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col("time")), "yyyy-MM-dd HH:mm:ss"))\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))

races_with_timestamp_df = add_ingestion_date(races_with_timestamp_df)
display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Select only the required columns.

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("ingestion_date"), col("race_timestamp"), col("data_source"))

# COMMAND ----------

# CREATING PARQUET FILE
races_selected_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{silver_folder_path}races")

# CREATING MANAGED TABLE
races_selected_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_catalog.f1_processed.races")

# CREATING EXTERNAL TABLE
races_selected_df.write.mode("overwrite").format("delta").option('path', 'abfss://container@storageAccount.dfs.core.windows.net/f1_catalog_ext/f1_processed_ext/races').saveAsTable("f1_catalog_ext.f1_processed_ext.races")

# COMMAND ----------

dbutils.notebook.exit("Success")