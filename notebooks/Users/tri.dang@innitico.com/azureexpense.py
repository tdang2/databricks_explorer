# Databricks notebook source
from pyspark.sql.functions import col, split, array_contains, lit, current_date, lower
from pyspark.sql.types import IntegerType
# Setup connection
spark.conf.set(
  "fs.azure.account.key." + dbutils.secrets.get(scope="scda-dev", key="scdablob-dev-account-name") + ".blob.core.windows.net",
  dbutils.secrets.get(scope="scda-dev", key="scdablobdev-access-key"))
# Read the files 
sc_dev_path = "wasbs://scfinance@" + dbutils.secrets.get(scope="scda-dev", key="scdablob-dev-account-name") + ".blob.core.windows.net/scdevmtd/dailyexpense"
sc_prod_path = "wasbs://scfinance@" + dbutils.secrets.get(scope="scda-dev", key="scdablob-dev-account-name") + ".blob.core.windows.net/scprodmtd/dailyexpense"
corp_dev_path = "wasbs://scfinance@" + dbutils.secrets.get(scope="scda-dev", key="scdablob-dev-account-name") + ".blob.core.windows.net/corpdevmtd/dailyexpense"
corp_prod_path = "wasbs://scfinance@" + dbutils.secrets.get(scope="scda-dev", key="scdablob-dev-account-name") + ".blob.core.windows.net/corpprodmtd/dailyexpense"

# COMMAND ----------

def latest_date_folder(path):
  df = sqlContext.createDataFrame(dbutils.fs.ls(path))
  s = df.describe(['name'])
  max_name = s.where(s['summary'] == 'max').select('name').collect()[0]['name']
  return df.where(df['name'] == max_name).collect()[0]['path']

# COMMAND ----------

def latest_file(path):
  df = sqlContext.createDataFrame(dbutils.fs.ls(path))
  s = df.describe(['size'])
  max_size = s.where(s['summary'] == 'max').select('size').collect()[0]['size']
  return df.where(df['size'] == max_size).collect()[0]['path']

# COMMAND ----------

@udf
def get_cost_center(tag_col_arr):  
  res = None
  for pair_str in tag_col_arr:
    pair = pair_str.split(":")
    if pair[0] == "\"cost_center_number\"":
      num_str = pair[1].replace("\"", "")
      try:
        res = int(pair[1].replace("\"", ""))
      except:
        res = ''
  return res

# COMMAND ----------

@udf
def get_team(tag_col_arr):  
  res = ''
  for pair_str in tag_col_arr:
    pair = pair_str.split(":")
    if pair[0] == "\"team\"":
      res = pair[1]
  return res

# COMMAND ----------

from pyspark.sql.functions import month, year
def process_expense_file(file_path):
  tempDf = spark.read.load(file_path, format="csv", header="true", escape='"')
  tempDf = tempDf.filter(tempDf["Tags"].isNotNull()).select(
    col("SubscriptionName"),
    col("ResourceGroup"),
    col("ResourceLocation"),
    col("ProductName"),
    col("PreTaxCost"),
    col("ResourceType"),
    col("UsageDateTime"),
    split(col("Tags"), ",").alias("tags")
  )
  return tempDf.withColumn("costCenterNumber", lit(get_cost_center(tempDf["tags"])))\
          .withColumn("team", lit(get_team(tempDf["tags"])))\
          .withColumn("usageMonth", month(tempDf['UsageDateTime']))\
          .withColumn("usageYear", year(tempDf['UsageDateTime']))\
          .withColumn("processedDate", current_date())\
          .select(
    col("processedDate"),
    col("SubscriptionName"),
    col("ResourceGroup"),
    col("ResourceLocation"),
    col("ProductName"),
    col("ResourceType"),
    col("costCenterNumber"),
    col("team"),
    col("usageMonth"),
    col("usageYear"),
    col("UsageDateTime"),
    col("PreTaxCost")
  ).filter("costCenterNumber is not NULL")

# COMMAND ----------

def convertLowerCase(df):
  for colName in ['ResourceGroup', 'ResourceLocation', 'ProductName', 'ResourceType', 'SubscriptionName']:
    df = df.withColumn(colName, lower(col(colName)));
  return df

# COMMAND ----------

def save_overwrite_unmanaged_table(df, tname, writeMode):
  """ Overwrite or create unmanaged tables. Databrick knows schema but data stored in ADLS"""
  path = "dbfs:/mnt/SupplyChain/dev/azureexpense/"
  df.write.partitionBy("SubscriptionName", "usageYear", "usageMonth", "costCenterNumber").mode(writeMode).format("parquet").option("path", path + tname).saveAsTable(tname)

# COMMAND ----------

def save_schema(df, path_file_name):
  schema = sc.parallelize(df.schema)
  dbutils.fs.rm(path_file_name, True)
  schema.saveAsPickleFile(path_file_name)

# COMMAND ----------

# MAGIC %md
# MAGIC First, clean current month to date data

# COMMAND ----------

from datetime import datetime
# Check whether the data exists
filedf = sqlContext.createDataFrame(dbutils.fs.ls("dbfs:/mnt/SupplyChain/dev/azureexpense/"))
if len(filedf.filter(filedf['name'] == 'sc_expense/').head(1)) > 0:
  dataDf = sqlContext.read.parquet("dbfs:/mnt/SupplyChain/dev/azureexpense/sc_expense")  
  m = datetime.now().month
  y = datetime.now().year    
  dataDf = dataDf.where((dataDf.usageMonth != m) & (dataDf.usageYear != y))
  if len(dataDf.head(1)) == 0:    
    dbutils.fs.rm("dbfs:/mnt/SupplyChain/dev/azureexpense/sc_expense",True)
  else:
    save_overwrite_unmanaged_table(dataDf, 'sc_expense', 'overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC Then process month to date data for this year. Month and Year are calendar frame (not fiscal). Be careful with month and year variable as it will overwrite month() and year() spark function

# COMMAND ----------

for ffolder in [sc_dev_path, sc_prod_path, corp_dev_path, corp_prod_path]:
  latest_folder = latest_date_folder(ffolder)
  lfile = latest_file(latest_folder)
  x = process_expense_file(lfile) 
  x = convertLowerCase(x)
  save_overwrite_unmanaged_table(x, 'sc_expense', 'append')
save_schema(x, "dbfs:/mnt/SupplyChain/schema/azureexpense/sc_expense")

# COMMAND ----------

loadDf = sqlContext.read.parquet("dbfs:/mnt/SupplyChain/dev/azureexpense/sc_expense")
display(loadDf)

# COMMAND ----------

from pyspark.sql import functions as F
loadDf.where((loadDf['SubscriptionName'] == 'supplychain-nonprod') & (loadDf['ResourceGroup'] == 'supplychain_scda_de_0')).groupBy().agg(F.sum('PreTaxCost')).collect()
