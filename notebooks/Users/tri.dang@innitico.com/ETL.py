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

df2 = df1.join(invmst_df_wdf, df1.SKU == invmst_df_wdf.INUMBR, 'inner').select(
  [col(x) for x in df1.columns] +
  [invmst_df_wdf.BYRNUM.alias("BUYER_NUMBER"), 
   invmst_df_wdf.ASNUM.alias("VENDOR_NUMBER"), 
   invmst_df_wdf.IDESCR.alias("SKU_DESCRIPTION"),
   invmst_df_wdf.IDSCCD.alias("NETWORK_MASTER_STATUS"),
  ]
)
df2.printSchema()

# COMMAND ----------

df3 = df2.join(apsupp_df, df2.VENDOR_NUMBER == apsupp_df.ASNUM, 'left').select(
  [col(x) for x in df2.columns] +
  [
    apsupp_df.ASNAME.alias("VENDOR_NAME")    
  ]
)
display(df3.head(5))

# COMMAND ----------

df4 = df3.join(inybal_df, (df3.SKU == inybal_df.INUMBR) & (df3.STORE_NUMBER == inybal_df.ISTORE), 'left').select(
  [col(x) for x in df3.columns] + [inybal_df.IBHAND.alias("ON_HAND")]
)
df4.fillna(0, subset=['ON_HAND'])

# COMMAND ----------

from pyspark.sql import functions as F
# Best practice to rename shared column name to avoid confusion
display_df = display_df.select(
  display_df.SKU_CLASS.alias("DISPLAY_SKU_CLASS"),
  display_df.DISPLAY_MODEL
)
df5 = df4.join(display_df, df4.SKU_CLASS == display_df.DISPLAY_SKU_CLASS, 'left').select(
  [col(x) for x in df4.columns] + [display_df.DISPLAY_MODEL.alias("DISPLAY_MODEL")]
)

# COMMAND ----------

# Best practice to rename shared column name to avoid confusion
out_lvl_df = out_lvl_df.select(
  out_lvl_df.SKU.alias("SKU_OUT_LEVEL"),
  out_lvl_df.OUT_AT_LVL
)
df6 = df5.join(out_lvl_df, df5.SKU == out_lvl_df.SKU_OUT_LEVEL, 'left').select(
  [col(x) for x in df5.columns] + [out_lvl_df.OUT_AT_LVL.alias("SKU_OUT_LEVEL")]
)
display(df6.head(5))

# COMMAND ----------

df7 = df6.withColumn("OUT_LEVEL_QTY", when(
  df6.SKU_OUT_LEVEL.isNull(),
    when(
          df6["DISPLAY_MODEL"] == 'Y', 1
        ).otherwise(0)
  ).otherwise(df6['SKU_OUT_LEVEL']))
df7 = df7.drop('SKU_OUT_LEVEL')
display(df7.head(5))

# COMMAND ----------

df8 = df7.withColumn("OUT_SKU_STOR", when(df7["ON_HAND"] <= df7["OUT_LEVEL_QTY"], 1).otherwise(0))
display(df8.head(5))

# COMMAND ----------

# Best practice to rename shared column name to avoid confusion
critical_df = critical_df.select(
  critical_df.SKU.alias("SKU_CRITICAL")
)
df9 = df8.join(critical_df, df8.SKU == critical_df.SKU_CRITICAL, 'left')
df9 = df9.withColumn("CRITICAL_IN_STOCK", when(df9["SKU_CRITICAL"].isNotNull(), 'Y').otherwise(df9["SKU_CRITICAL"]))
df9 = df9.drop('SKU_CRITICAL')
display(df9)

# COMMAND ----------

save_overwrite_unmanaged_table(df9, "retail_sku")

# COMMAND ----------

