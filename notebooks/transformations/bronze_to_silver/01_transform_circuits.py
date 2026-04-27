# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

# Creating widgets for parameter passing into the notebook
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

from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Cell 6
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("country", StringType(), True),
                                    StructField("lat", DoubleType(), True),
                                    StructField("lng", DoubleType(), True),
                                    StructField("alt", IntegerType(), True),
                                    StructField("url", StringType(), True)
                                   ])

# COMMAND ----------

# circuits_df = spark.read\
#       .option("header", True)\
#       .option('inferSchema', True)\
#       .csv("abfss://raw@databrickswsj01stg.dfs.core.windows.net/circuits.csv")

# so in place of inferSchema we will be using user created schema via structType and structField

circuits_df = spark.read\
      .option("header", True)\
      .schema(circuits_schema)\
      .csv(f"{bronze_folder_path}/{v_file_date}/circuits.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC * So when we enables inferSchema sparks goes through the data and checks for the schema of data and applies appropriate schema to it.
# MAGIC * Due to this one extra job is created, which is not efficient, as it has to read through the data.
# MAGIC * So in prod env we should not use this because its going to slow our process
# MAGIC * So as an alternative we can use structFiels and StructType to create schema for our table i.e. one way of doing it.
# MAGIC * There is also DDL way to create schema which we will see later.
# MAGIC * StructType = This is our row.
# MAGIC * StructFields = This is the fields in our row i.e. column datatype.

# COMMAND ----------

# MAGIC %md
# MAGIC lets check the type of df

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC display() cmd will show the data in more readable format with all the values when compared with show() cmd

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC describle() function will tell us the min and max values for the columns and helps in verify the data types

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Select only the required cloumns

# COMMAND ----------

# circuits_selected_df = circuits_df.select('circuitId','circuitRef','name','location','country','lat','lng','alt')
# display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### another way of using select statement
# MAGIC * df_name.select(df_name.co1, df_name.col2, ...)
# MAGIC
# MAGIC * using select( col(col_name),...) this type of function gives you more flexibility, we can use other functions as well, such as alias

# COMMAND ----------

# circuits_selected_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

# circuits_selected_df = circuits_df.select(circuits_df["circuitId"] , circuits_df["circuitRef"] , circuits_df["name"] ,circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt'))
display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3 - As per requirment We need to rename the cols

# COMMAND ----------

from pyspark.sql.functions import lit
circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId', 'circuit_id') \
                                          .withColumnRenamed('circuitRef', 'circuit_ref') \
                                          .withColumnRenamed('lat', 'latitude') \
                                          .withColumnRenamed('lng', 'longitude') \
                                          .withColumnRenamed('alt', 'altitude')\
                                          .withColumn("data_source", lit(v_data_source))\
                                          .withColumn("file_date", lit(v_file_date))
display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Add ingestion date to the dataframe
# MAGIC
# MAGIC * withColumnRenamed function helps you to rename existing column
# MAGIC * withColumn function helps you to create new column with in df

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# normal way: - 
# circuits_final_df = circuits_renamed_df.withColumn('ingestion_date', current_timestamp

# Using created function: -
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add new col with litral value
# MAGIC
# MAGIC * you can do this with the help of lit() function inside withcolumn, eg: -

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# circuits_final02_df = circuits_final_df.withColumn('env', lit('production'))
# display(circuits_final02_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Write file and table data to datalake.

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

circuits_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/circuits")

#creating a managed table
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_catalog.f1_processed.circuits")

#creating external table
circuits_final_df.write.mode("overwrite").format("delta").option('path', 'abfss://container@storageAccount.dfs.core.windows.net/f1_catalog_ext/f1_processed_ext/circuits').saveAsTable("f1_catalog_ext.f1_processed_ext.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")