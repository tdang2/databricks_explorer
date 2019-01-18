# Databricks notebook source
from pyspark.sql.functions import col, split, array_contains, lit, current_date
from pyspark.sql.types import IntegerType
# Setup connection
spark.conf.set(
  "fs.azure.account.key." + dbutils.secrets.get(scope="scda-dev", key="scdablob-dev-account-name") + ".blob.core.windows.net",
  dbutils.secrets.get(scope="scda-dev", key="scdablobdev-access-key"))
# Read the files
sc_dev_path = "wasbs://scfinance@" + dbutils.secrets.get(scope="scda-dev", key="scdablob-dev-account-name") + ".blob.core.windows.net/azuredev/weeklyscexpense"
sc_prod_path = "wasbs://scfinance@" + dbutils.secrets.get(scope="scda-dev", key="scdablob-dev-account-name") + ".blob.core.windows.net/azureprod/weeklyscexpense"
corp_dev_path = "wasbs://scfinance@" + dbutils.secrets.get(scope="scda-dev", key="scdablob-dev-account-name") + ".blob.core.windows.net/azuredev/weeklycorpexpense"
corp_prod_path = "wasbs://scfinance@" + dbutils.secrets.get(scope="scda-dev", key="scdablob-dev-account-name") + ".blob.core.windows.net/azureprod/weeklycorpexpense"

# COMMAND ----------

def latest_week_folder(path):
  df = sqlContext.createDataFrame(dbutils.fs.ls(path))
  s = df.describe(['name'])
  max_name = s.where(s['summary'] == 'max').select('name').collect()[0]['name']
  return df.where(df['name'] == max_name).collect()[0]['path']

# COMMAND ----------

def latest_week_file(path):
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
      res = int(pair[1].replace("\"", ""))
  return res

# COMMAND ----------

@udf
def get_team(tag_col_arr):  
  res = None
  for pair_str in tag_col_arr:
    pair = pair_str.split(":")
    if pair[0] == "\"team\"":
      res = pair[1]
  return res

# COMMAND ----------

def process_expense_file(file_path):
  df = spark.read.load(file_path, format="csv", header="true", escape='"')
  df = df.filter(df["Tags"].isNotNull()).select(
    col("SubscriptionName"),
    col("ResourceGroup"),
    col("ResourceLocation"),
    col("ProductName"),
    col("PreTaxCost"),
    col("ResourceType"),
    split(col("Tags"), ",").alias("tags")
  )
  res = df.withColumn("cost_center_number", lit(get_cost_center(df["tags"])))\
          .withColumn("team", lit(get_team(df["tags"])))\
          .withColumn("processed_date", current_date())\
          .select(["processed_date", "SubscriptionName", "ResourceGroup", "ResourceLocation", "ProductName", "PreTaxCost", "ResourceType", "cost_center_number", "team"])
  res = res.filter(res["cost_center_number"].isNotNull())
  return res

# COMMAND ----------

def save_overwrite_unmanaged_table(df, tname, writeMode):
  """ Overwrite or create unmanaged tables. Databrick knows schema but data stored in ADLS"""
  path = "dbfs:/mnt/SupplyChain/dev/azureexpense/"
  df.write.partitionBy("SubscriptionName", "processed_date", "cost_center_number", "team").mode(writeMode).format("parquet").option("path", path + tname).saveAsTable(tname)

# COMMAND ----------

def save_schema(df, path_file_name):
  schema = sc.parallelize(df.schema)
  dbutils.fs.rm(path_file_name, True)
  schema.saveAsPickleFile(path_file_name)

# COMMAND ----------

latest_folder = latest_week_folder(sc_dev_path)
latest_file = latest_week_file(latest_folder)
df = process_expense_file(latest_file)
save_overwrite_unmanaged_table(df, 'weekly_expense', 'overwrite')
save_schema(df, "dbfs:/mnt/SupplyChain/schema/azureexpense/weekly_expense")

# COMMAND ----------

result = sqlContext.read.parquet("dbfs:/mnt/SupplyChain/dev/azureexpense/weekly_expense")
display(result)

# COMMAND ----------

schema = sc.pickleFile("dbfs:/mnt/SupplyChain/schema/azureexpense/weekly_expense")
schema.collect()

# COMMAND ----------

latest_folder = latest_week_folder(sc_prod_path)
latest_file = latest_week_file(latest_folder)
df = process_expense_file(latest_file)
save_overwrite_unmanaged_table(df, 'weekly_expense', 'append')

# COMMAND ----------

latest_folder = latest_week_folder(corp_dev_path)
latest_file = latest_week_file(latest_folder)
df = process_expense_file(latest_file)
save_overwrite_unmanaged_table(df, 'weekly_expense', 'append')

# COMMAND ----------

latest_folder = latest_week_folder(corp_prod_path)
latest_file = latest_week_file(latest_folder)
df = process_expense_file(latest_file)
save_overwrite_unmanaged_table(df, 'weekly_expense', 'append')

# COMMAND ----------

result = sqlContext.read.parquet("dbfs:/mnt/SupplyChain/dev/azureexpense/weekly_expense")
display(result)

# COMMAND ----------


