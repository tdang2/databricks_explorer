# Databricks notebook source
import pandas as pd
import numpy as np
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# COMMAND ----------

spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "1d76bb0b-c509-49ff-8639-23c5e1bb9b4d")
spark.conf.set("dfs.adls.oauth2.credential", "K/k2d/Ane1/mvghYsY1tbE0/6xTdP/92uQafJMYTXnQ=")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/c622fc2d-f138-46eb-a729-3aa0cce44410/oauth2/token")

# COMMAND ----------

def load_file(f_name):
  path = "adl://staplesazurepoc.azuredatalakestore.net/poc_csv/"
  return sqlContext.read.format('csv').options(header='true', inferSchema='true').load(path + f_name)

# COMMAND ----------

apsupp_df = load_file('GALAXY_MMBASLIB_APSUPP_V.csv')
coystr_df = load_file('GALAXY_MMBASLIB_COYSTR_V.csv')
invmst_df = load_file('GALAXY_MMBASLIB_INVMST_V.csv')
inybal_df = load_file('GALAXY_MMBASLIB_INYBAL_V.csv')
rplstdet_df = load_file('GALAXY_MMBASLIB_RPLSTDET_V.csv')
display_df = load_file('RETAIL_SKU_CLASS_DISPLAY_MODEL_T.csv')
critical_df = load_file('RETAIL_SKU_CRITICAL_IN_STOCK_T.csv')
out_lvl_df = load_file('RETAIL_SKU_OUT_AT_LVL_T.csv')
inystr_df = load_file('GALAXY_MMBASLIB_INYSTR_V.csv')

# COMMAND ----------

def save_overwrite_unmanaged_table(df, tname):
  """ Overwrite or create unmanaged tables. Databrick knows schema but data stored in ADLS"""
  path = "adl://staplespocnoencryption.azuredatalakestore.net/poc_table/" 
  df.write.mode('overwrite').format("parquet").option("path", path + tname).saveAsTable(tname)

# COMMAND ----------

save_overwrite_unmanaged_table(apsupp_df, "apsupp")
save_overwrite_unmanaged_table(coystr_df, "coystr")
save_overwrite_unmanaged_table(invmst_df, "invmst")
save_overwrite_unmanaged_table(inybal_df, "inybal")
save_overwrite_unmanaged_table(rplstdet_df, "rplstdet")
save_overwrite_unmanaged_table(display_df, "display")
save_overwrite_unmanaged_table(critical_df, "critical")
save_overwrite_unmanaged_table(out_lvl_df, "out_lvl")
save_overwrite_unmanaged_table(inystr_df, "inystr")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM apsupp LIMIT 10;

# COMMAND ----------

# Read dataframe parquet file from ADLS
apsupp_df2 = sqlContext.read.parquet("adl://staplespocnoencryption.azuredatalakestore.net/poc_table/apsupp")

# COMMAND ----------

# MAGIC %md 
# MAGIC Cant run this. Q: is this because all operation on spark dataframe is lazy thus we can't convert to python pandas? 
# MAGIC 
# MAGIC <code>
# MAGIC apsupp_df2.select("*").toPandas()
# MAGIC </code>

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANT ON TABLE apsupp;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP VIEW IF EXISTS restrictedview1;
# MAGIC CREATE VIEW restrictedview1 AS SELECT ASNUM, ASNAME FROM apsupp where ASNUM > 100000 and ASNUM < 999999;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANT ON restrictedview1;

# COMMAND ----------

# MAGIC %sql 
# MAGIC REVOKE ALL PRIVILEGES ON DATABASE default FROM `user1@tridanginnitico.onmicrosoft.com`;
# MAGIC GRANT SELECT ON VIEW restrictedview1 TO `user1@tridanginnitico.onmicrosoft.com`;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW GRANT ON restrictedview1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM restrictedview1 LIMIT 10;

# COMMAND ----------

