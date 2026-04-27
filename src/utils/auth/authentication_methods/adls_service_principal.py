# Databricks notebook source
# MAGIC %md
# MAGIC ***Access Azure Data Lake using Service Principle***
# MAGIC 1. Register Azure AD/Service Principle
# MAGIC 2. Generate a secret/password for the application
# MAGIC 3. Set spark config with app/client id, directory/tenant id & secret
# MAGIC 4. Read Data from circuits.csv file

# COMMAND ----------

# client_id = "client_id"
# tenant_id = "tenant_id"
# client_secret = "key_secret"

# COMMAND ----------

# MAGIC %md
# MAGIC using AKV in place of providing hard coded values.

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key ='formuladl-service-principle-client-id')

tenant_id = dbutils.secrets.get(scope='formula1-scope', key ='formuladl-service-principle-tenant-id')

client_secret = dbutils.secrets.get(scope="formula1-scope", key="formuladl-service-principle-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databrickswsj01stg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.databrickswsj01stg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.databrickswsj01stg.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.databrickswsj01stg.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.databrickswsj01stg.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC