# Databricks notebook source
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
  path = "adl://staplespocnoencryption.azuredatalakestore.net/poc_table_etl/" 
  df.write.mode('overwrite').format("parquet").option("path", path + tname).saveAsTable(tname)


# COMMAND ----------

save_overwrite_unmanaged_table(apsupp_df, "apsupp2")
save_overwrite_unmanaged_table(coystr_df, "coystr2")
save_overwrite_unmanaged_table(invmst_df, "invmst2")
save_overwrite_unmanaged_table(inybal_df, "inybal2")
save_overwrite_unmanaged_table(rplstdet_df, "rplstdet2")
save_overwrite_unmanaged_table(display_df, "display2")
save_overwrite_unmanaged_table(critical_df, "critical2")
save_overwrite_unmanaged_table(out_lvl_df, "out_lvl2")
save_overwrite_unmanaged_table(inystr_df, "inystr2")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM apsupp2 LIMIT 10;

# COMMAND ----------

inystr_wdf = inystr_df.filter(
  ((inystr_df.ISSTAT == 'A') | (inystr_df.ISSTAT == 'P')) &
  ((inystr_df.IDEPT == 1) | (inystr_df.IDEPT == 2) | (inystr_df.IDEPT == 3) | (inystr_df.IDEPT == 5) | (inystr_df.IDEPT == 7)) &
  ((inystr_df.ISPACT == 'A') | (inystr_df.ISDACT == 'A'))
)

# COMMAND ----------

rplstdet_wdf = rplstdet_df.filter(
  rplstdet_df.SSETN == 83
).select(rplstdet_df.SSETN, rplstdet_df.SSTORE, rplstdet_df.SGRPCD)

# COMMAND ----------

invmst_df_wdf = invmst_df.filter(
  ((invmst_df.IDSCCD == 'A') | (invmst_df.IDSCCD == 'P')) & 
  (invmst_df.ISTYPE == 'OH')
)

# COMMAND ----------

from pyspark.sql.functions import *
df1 = inystr_wdf.join(rplstdet_wdf, inystr_wdf.ISTORE == rplstdet_wdf.SSTORE, 'inner').select(
    inystr_wdf.ISTORE.alias("STORE_NUMBER"), 
    inystr_wdf.INUMBR.alias("SKU"), 
    inystr_wdf.IDEPT.alias("SKU_DIVISION"), 
    inystr_wdf.ISDEPT.alias("SKU_DEPARTMENT"), 
    inystr_wdf.ICLAS.alias("SKU_CLASS"), 
    inystr_wdf.ISSTAT.alias("MERCHANDISING_STATUS"), 
    inystr_wdf.ISDACT.alias("SOURCING_STATUS"), 
    inystr_wdf.ISPACT.alias("PURCHASE_STATUS"),
    col("ISPOG#").alias("POG_ID")
  )
df1.printSchema()

# COMMAND ----------

