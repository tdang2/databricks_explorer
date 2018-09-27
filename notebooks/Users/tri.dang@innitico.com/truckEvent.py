# Databricks notebook source
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "1d0ac982-d44a-4b20-8872-0002ea256723",
           "dfs.adls.oauth2.credential": "iiBYLJqGVlAzxmw5HTc+kr8lgQOO4NXDT9DzKjWXiDU=",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/b101f7ab-56ac-485f-b397-5279698fdf7d/oauth2/token"}
# This to set up the mount at the beginning. Only need to do it once
# dbutils.fs.mount(
#   source = "adl://staplessupplychain.azuredatalakestore.net/truckEvents",
#   mount_point = "/mnt/truckEvents",
#   extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/truckEvents"))

# COMMAND ----------

# MAGIC %fs head /mnt/truckEvents/truckEvents_1537991537757

# COMMAND ----------

from pyspark.sql.types import *
# Since we know the data format already, let's define the schema to speed up processing (no need for Spark to infer schema)
jsonSchema = StructType([ 
  StructField("eventTime", TimestampType(), False), 
  StructField("truckId", StringType(), False),
  StructField("driverId", StringType(), False),
  StructField("latitude", StringType(), False),  # data is in string format
  StructField("longitude", StringType(), False), # data is in string format
  StructField("state", StringType(), False),
  StructField("partitionnumber", IntegerType(), False),
  StructField("kafkaoffset", LongType(), False)
])

# COMMAND ----------

inputPath = "/mnt/truckEvents/"
# Static DataFrame representing data in the JSON files
staticInputDF = (
  spark
    .read
    .schema(jsonSchema)
    .json(inputPath)
)

display(staticInputDF)

# COMMAND ----------

# Convert type from string to integer, double
staticInputDF = staticInputDF.withColumn("truckId", staticInputDF["truckId"].cast(IntegerType()))
staticInputDF = staticInputDF.withColumn("driverId", staticInputDF["driverId"].cast(IntegerType()))
staticInputDF = staticInputDF.withColumn("latitude", staticInputDF["latitude"].cast(DoubleType()))
staticInputDF = staticInputDF.withColumn("longitude", staticInputDF["longitude"].cast(DoubleType()))

# COMMAND ----------

display(staticInputDF)

# COMMAND ----------

# Now we do streaming
from pyspark.sql.functions import *

# Similar to definition of staticInputDF above, just using `readStream` instead of `read`
streamingInputDF = (
  spark
    .readStream                       
    .schema(jsonSchema)                # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 10)  # Treat a sequence of files as a stream by picking 10 files at a time
    .json(inputPath)
)

# Counting group by state
streamingCountsDF = (                 
  streamingInputDF
    .withWatermark("eventTime", "1 minute")
    .groupBy(
      streamingInputDF.state, 
      window(streamingInputDF.eventTime, "1 minute"))
    .count()
)

# Is this DF actually a streaming DF?
streamingCountsDF.isStreaming

# COMMAND ----------

query = (streamingCountsDF
  .writeStream
  .format("memory")                     # memory = store in-memory table (for testing only in Spark 2.0)  
  .queryName("streamingTruckCount")     # streamingTruckCount = name of the in-memory table
  .trigger(processingTime='1 minute')
  .outputMode("update")                 # complete = all the counts should be in the table
  .start()
)

# COMMAND ----------

query.recentProgress

# COMMAND ----------

# MAGIC %sql select * from streamingTruckCount order by date_format(window.start, "MMM-dd HH:mm") DESC

# COMMAND ----------

# MAGIC %sql select * from streamingTruckCount order by date_format(window.start, "MMM-dd HH:mm") DESC

# COMMAND ----------


