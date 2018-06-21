// Databricks notebook source
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "1d76bb0b-c509-49ff-8639-23c5e1bb9b4d")
spark.conf.set("dfs.adls.oauth2.credential", "K/k2d/Ane1/mvghYsY1tbE0/6xTdP/92uQafJMYTXnQ=")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/c622fc2d-f138-46eb-a729-3aa0cce44410/oauth2/token")

// COMMAND ----------

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions.{ explode, split }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val connectionString = ConnectionStringBuilder("Endpoint=sb://mean-world.servicebus.windows.net/;SharedAccessKeyName=databricks;SharedAccessKey=tK/UCRwPCFMd00zJgcSzWjtkGKQSU97eAYnIUYtjkis=;EntityPath=mean-world-app")
  .setEventHubName("mean-world-app")
  .build
val eventHubsConf = EventHubsConf(connectionString)
//  .setStartingPosition(EventPosition.fromEndOfStream)
  
val eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

// COMMAND ----------

eventhubs.printSchema

// COMMAND ----------

// Event Hub message format is JSON and contains "body" field
// Body is binary, so we cast it to string to see the actual content of the message
val messages = eventhubs
  .withColumn("Body", $"body".cast(StringType))

// COMMAND ----------

// The body column is provided as a binary. Applying the cast("string") operation, to retrieve data
// must have some groupby transformation
var df = eventhubs.select(
  get_json_object(($"body").cast("string"), "$.data.isbn").alias("isbn"),
  get_json_object(($"body").cast("string"), "$.data.title").alias("title"),
  get_json_object(($"body").cast("string"), "$.data.author").alias("author"),
  get_json_object(($"body").cast("string"), "$.data.description").alias("description"),
  get_json_object(($"body").cast("string"), "$.data.published_year").alias("published_year"),
  get_json_object(($"body").cast("string"), "$.data.publisher").alias("publisher"),
  get_json_object(($"body").cast("string"), "$.data.updated_date").alias("updated_at"),
  unix_timestamp(
        get_json_object(($"body").cast("string"), "$.data.updated_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
      ).cast(TimestampType).alias("time")
)
//.withWatermark("time", "5 minutes")
//.groupBy($"author", $"published_year", window($"time", "2 minutes"))
//.groupBy($"author", $"published_year")
//.count()

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger.ProcessingTime

val query = df.writeStream
    .format("memory")
    .queryName("streamTest")
    //.outputMode("append") 
    //.trigger(ProcessingTime("60 seconds"))
    .start()

// COMMAND ----------

val query = df.writeStream
    .format("json")
    .option("path", "adl://staplesazurepoc.azuredatalakestore.net/poc_csv/book_checkpoint_json2")
    .option("checkpointLocation", "adl://staplesazurepoc.azuredatalakestore.net/poc_csv/book_stream_json2")
    .partitionBy("author")
    .trigger(ProcessingTime("60 seconds"))
    .start()


// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM streamTest;

// COMMAND ----------

