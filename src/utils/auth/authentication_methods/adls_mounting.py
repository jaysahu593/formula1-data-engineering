# Databricks notebook source
# MAGIC %md
# MAGIC ***Access Azure Data Lake using Service Principle***
# MAGIC 1. Register Azure AD/Service Principle
# MAGIC 2. Generate a secret/password for the application
# MAGIC 3. Set spark config with app/client id, directory/tenant id & secret
# MAGIC 4. Read Data from circuits.csv file

# COMMAND ----------

# client_id = "client_id/application_id"
# tenant_id = "tenant_id"
# client_secret = "secret_value"

# COMMAND ----------

# MAGIC %md
# MAGIC using AKV in place of providing hard coded values.

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key ='formuladl-service-principle-client-id')

tenant_id = dbutils.secrets.get(scope='formula1-scope', key ='formuladl-service-principle-tenant-id')

client_secret = dbutils.secrets.get(scope="formula1-scope", key="formuladl-service-principle-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.storageAccount.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.storageAccount.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.storageAccount.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.storageAccount.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.storageAccount.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("abfss://container@storageAccount.dfs.core.windows.net/circuits.csv")


df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
 
# Optionally, you can add <directory-name> to the source URI of your mount point.


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://dbfs@storageAccount.dfs.core.windows.net/",
  mount_point = "/mnt/databrickswsj01stg/dbfs",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - Mounting is failing with error that mount() is not whitelisted
# MAGIC - This is bcoz databricks has disbaled this feature for security reason.
# MAGIC - And also the DBFS is disabled in unity-catalog enabled databricks workspace.

# COMMAND ----------

