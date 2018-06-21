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

apsupp_df = sqlContext.read.parquet("adl://staplespocnoencryption.azuredatalakestore.net/poc_table/apsupp")
coystr_df = sqlContext.read.parquet("adl://staplespocnoencryption.azuredatalakestore.net/poc_table/coystr")
invmst_df = sqlContext.read.parquet("adl://staplespocnoencryption.azuredatalakestore.net/poc_table/invmst")
inybal_df = sqlContext.read.parquet("adl://staplespocnoencryption.azuredatalakestore.net/poc_table/inybal")
rplstdet_df = sqlContext.read.parquet("adl://staplespocnoencryption.azuredatalakestore.net/poc_table/rplstdet")
display_df = sqlContext.read.parquet("adl://staplespocnoencryption.azuredatalakestore.net/poc_table/display")
critical_df = sqlContext.read.parquet("adl://staplespocnoencryption.azuredatalakestore.net/poc_table/critical")
out_lvl_df = sqlContext.read.parquet("adl://staplespocnoencryption.azuredatalakestore.net/poc_table/out_lvl")
inystr_df = sqlContext.read.parquet("adl://staplespocnoencryption.azuredatalakestore.net/poc_table/inystr")

# COMMAND ----------

# Saving as databricks managed table: databricks stores both data and schema
apsupp_df.write.mode('overwrite').format("parquet").saveAsTable('apsupp')
coystr_df.write.mode('overwrite').format("parquet").saveAsTable('coystr')
invmst_df.write.mode('overwrite').format("parquet").saveAsTable('invmst')
inybal_df.write.mode('overwrite').format("parquet").saveAsTable('inybal')
inystr_df.write.mode('overwrite').format("parquet").saveAsTable('inystr')
rplstdet_df.write.mode('overwrite').format("parquet").saveAsTable('rplstdet')
display_df.write.mode('overwrite').format("parquet").saveAsTable('display')
critical_df.write.mode('overwrite').format("parquet").saveAsTable('critical')
out_lvl_df.write.mode('overwrite').format("parquet").saveAsTable('out_lvl')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANT ON TABLE apsupp;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP VIEW IF EXISTS restrictedview2;
# MAGIC CREATE VIEW restrictedview2 AS SELECT ASNUM, ASNAME FROM apsupp where ASNUM > 100000 and ASNUM < 999999;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANT ON apsupp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANT ON restrictedview2;

# COMMAND ----------

# MAGIC %sql 
# MAGIC REVOKE ALL PRIVILEGES ON DATABASE default FROM `user1@tridanginnitico.onmicrosoft.com`;
# MAGIC REVOKE ALL PRIVILEGES ON TABLE apsupp FROM `user1@tridanginnitico.onmicrosoft.com`;
# MAGIC ALTER TABLE restrictedview2 OWNER TO `tri.dang@innitico.com`;
# MAGIC GRANT SELECT ON VIEW restrictedview2 TO `user1@tridanginnitico.onmicrosoft.com`;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW GRANT ON restrictedview2;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW GRANT `user1@tridanginnitico.onmicrosoft.com` ON TABLE coystr;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED apsupp;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED restrictedview2

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW GRANT ON TABLE apsupp;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED apsupp;