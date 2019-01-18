# Databricks notebook source
# MAGIC %md
# MAGIC # Description
# MAGIC This is an example of setting up databricks using Azure key vault to prevent exposing secrets to the public.
# MAGIC 
# MAGIC To use Azure secret feature, refer to this: https://docs.azuredatabricks.net/user-guide/secrets/secrets.html#secrets
# MAGIC 
# MAGIC Please note that by using Azure vault, databricks can only read the secret. Creating and maintaining secret will be on the Azure side

# COMMAND ----------

# Secret is read but hidden as intended
dbutils.secrets.get(scope = "scda-dev", key = "databricksDevSmallClientID")

# COMMAND ----------

dbutils.secrets.get(scope = "scda-dev", key = "staplesTenantId")

# COMMAND ----------

refresh_url = "https://login.microsoftonline.com/" + dbutils.secrets.get(scope = "scda-dev", key = "staplesTenantId") + "/oauth2/token"
print(refresh_url)

# COMMAND ----------

# Use this example to access ADLSv1 per notebook level
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", dbutils.secrets.get(scope = "scda-dev", key = "databricksDevSmallClientID"))
spark.conf.set("dfs.adls.oauth2.credential", dbutils.secrets.get(scope = "scda-dev", key = "ADdatabricksprodadla001Key"))
spark.conf.set("dfs.adls.oauth2.refresh.url", refresh_url)

# COMMAND ----------

display(dbutils.fs.ls("adl://<adsl-account-name>.azuredatalakestore.net/bronze/odl_soms_raw"))

# COMMAND ----------

# Only need to mount once
# dbutils.fs.mount(
#   source = "adl://<adsl-account-name>.azuredatalakestore.net/bronze/",
#   mount_point = "/mnt/bronze",
#   extra_configs = {
#     "dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
#     "dfs.adls.oauth2.client.id": dbutils.secrets.get(scope = "scda-dev", key = "databricksDevSmallClientID"),
#     "dfs.adls.oauth2.credential": dbutils.secrets.get(scope = "scda-dev", key = "ADdatabricksprodadla001Key"),
#     "dfs.adls.oauth2.refresh.url": refresh_url,
#   }
# )

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/bronze"))

# COMMAND ----------

dbutils.fs.mount(
  source = "adl://<adsl-account-name>.azuredatalakestore.net/SupplyChain/",
  mount_point = "/mnt/SupplyChain",
  extra_configs = {
    "dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
    "dfs.adls.oauth2.client.id": dbutils.secrets.get(scope = "scda-dev", key = "databricksDevSmallClientID"),
    "dfs.adls.oauth2.credential": dbutils.secrets.get(scope = "scda-dev", key = "ADdatabricksprodadla001Key"),
    "dfs.adls.oauth2.refresh.url": refresh_url,
  }
)

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/mnt/SupplyChain/schema/")
dbutils.fs.mkdirs("dbfs:/mnt/SupplyChain/dev/")
dbutils.fs.mkdirs("dbfs:/mnt/SupplyChain/staging/")
dbutils.fs.mkdirs("dbfs:/mnt/SupplyChain/prod/")

# COMMAND ----------

# Create and remove folder in SupplyChain mount point
dbutils.fs.mkdirs("dbfs:/mnt/SupplyChain/foobar/")
dbutils.fs.rm("dbfs:/mnt/SupplyChain/foobar")

# COMMAND ----------


