// Databricks notebook source
import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions.{ explode, split }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val connectionString = ConnectionStringBuilder("Endpoint=sb://mean-world.servicebus.windows.net/;SharedAccessKeyName=databricks;SharedAccessKey=tK/UCRwPCFMd00zJgcSzWjtkGKQSU97eAYnIUYtjkis=;EntityPath=mean-world-app")
  .setEventHubName("mean-world-app")
  .build
val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)
  
val eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

// COMMAND ----------

eventhubs.printSchema

// COMMAND ----------

// Sending the incoming stream into the console.
// Data comes in batches!
eventhubs.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

// Event Hub message format is JSON and contains "body" field
// Body is binary, so we cast it to string to see the actual content of the message
val messages = eventhubs
  .withColumn("Body", $"body".cast(StringType))
  .select("Body", "ISBN")

messages.printSchema

messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

// The body column is provided as a binary. Applying the cast("string") operation, to retrieve data
// must have some groupby transformation
var df = eventhubs.select(
  get_json_object(($"body").cast("string"), "$.data.isbn").alias("isbn"),
  get_json_object(($"body").cast("string"), "$.data.title").alias("title"),
  get_json_object(($"body").cast("string"), "$.data.author").alias("author"),
  get_json_object(($"body").cast("string"), "$.data.description").alias("description"),
  get_json_object(($"body").cast("string"), "$.data.published_year").alias("published_year"),
  get_json_object(($"body").cast("string"), "$.data.publisher").alias("publisher")
).groupBy($"author", $"published_year").count()

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger.ProcessingTime

val query = df.writeStream
    .format("console")        
    .outputMode("complete") 
    .trigger(ProcessingTime("60 seconds"))
    .start().awaitTermination()

// COMMAND ----------

