# Databricks notebook source
# MAGIC %md
# MAGIC ***Access Azure Data Lake using SAS Token***
# MAGIC 1. Set Spark configuraiton for SAS Token
# MAGIC 2. List files form demo container
# MAGIC 3. Read Data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.storageAccount.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.storageAccount.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.storageAccount.dfs.core.windows.net", "Token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@storageAccount.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Its failing because in Storage acc, access using key is disabled for us. But this is the process to connect
# MAGIC Similarly its failing for SAS token because it also follows key

# COMMAND ----------

