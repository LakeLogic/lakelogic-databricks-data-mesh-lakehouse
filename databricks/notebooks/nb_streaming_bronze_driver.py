# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Notebook  : Streaming Bronze Driver — Event Hub Consumer
# Purpose   : Reads real-time events from Azure Event Hub using
#             Spark Structured Streaming, routes them by entity type,
#             and writes to the ADLS landing zone in the standard
#             LakeLogic partition format.
#
#             This notebook does NOT run LakeLogic directly — it is
#             purely an ingestion bridge between Event Hub and the
#             landing zone. The batch/continuous pipeline driver then
#             picks up these files for Medallion processing.
#
# Widgets:
#   event_hub_connection  : Event Hub namespace connection string
#   event_hub_name        : Event Hub name (default: eh-rideflow-marketplace)
#   consumer_group        : Consumer group (default: $Default)
#   output_path           : Landing zone root (e.g., /Volumes/{catalog}/nondelta/landing_marketplace/rideflow)
#   checkpoint_path       : Structured Streaming checkpoint location
#   trigger_interval      : Processing interval (default: "30 seconds")
#
# Phase 2 component — activate after Event Hub is provisioned.
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Widgets

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("event_hub_connection", "", "Event Hub Connection String")
dbutils.widgets.text("event_hub_name", "eh-rideflow-marketplace", "Event Hub Name")
dbutils.widgets.text("consumer_group", "$Default", "Consumer Group")
dbutils.widgets.text("output_path",
    "/Volumes/rideflow_dev_demo/nondelta/_landing/marketplace/rideflow",
    "Output Path (Landing Zone)",
)
dbutils.widgets.text("checkpoint_path",
    "/Volumes/rideflow_dev_demo/nondelta/_checkpoints/streaming_bronze",
    "Checkpoint Path",
)
dbutils.widgets.text("trigger_interval", "30 seconds", "Trigger Interval")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Imports & Config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

EVENT_HUB_CONNECTION = dbutils.widgets.get("event_hub_connection").strip()
EVENT_HUB_NAME = dbutils.widgets.get("event_hub_name").strip()
CONSUMER_GROUP = dbutils.widgets.get("consumer_group").strip()
OUTPUT_PATH = dbutils.widgets.get("output_path").strip()
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path").strip()
TRIGGER_INTERVAL = dbutils.widgets.get("trigger_interval").strip() or "30 seconds"

# Build Event Hub connection config
eh_conf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        f"{EVENT_HUB_CONNECTION};EntityPath={EVENT_HUB_NAME}"
    ),
    "eventhubs.consumerGroup": CONSUMER_GROUP,
    "eventhubs.startingPosition": '{"offset": "-1", "seqNo": -1, "enqueuedTime": null, "isInclusive": true}',
    "maxEventsPerTrigger": 10000,
}

print(f"Event Hub:   {EVENT_HUB_NAME}")
print(f"Consumer:    {CONSUMER_GROUP}")
print(f"Output:      {OUTPUT_PATH}")
print(f"Checkpoint:  {CHECKPOINT_PATH}")
print(f"Trigger:     {TRIGGER_INTERVAL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌊 Structured Streaming — Event Hub → Landing Zone

# COMMAND ----------

# Read from Event Hub
raw_stream = (
    spark.readStream
    .format("eventhubs")
    .options(**eh_conf)
    .load()
)

# Parse the Event Hub message body (JSON string → columns)
parsed_stream = (
    raw_stream
    .select(
        F.col("body").cast(StringType()).alias("json_body"),
        F.col("enqueuedTime").alias("enqueued_time"),
        F.col("partitionKey").alias("entity"),
    )
    # Extract the _entity field from JSON for routing
    .withColumn("entity", F.coalesce(
        F.col("entity"),
        F.get_json_object(F.col("json_body"), "$._entity"),
    ))
    # Add time-based partition columns
    .withColumn("year", F.date_format(F.col("enqueued_time"), "yyyy"))
    .withColumn("month", F.date_format(F.col("enqueued_time"), "MM"))
    .withColumn("day", F.date_format(F.col("enqueued_time"), "dd"))
    .withColumn("hour", F.date_format(F.col("enqueued_time"), "HH"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 Write to Landing Zone (Partitioned by Entity + Time)

# COMMAND ----------

# Write to landing zone, partitioned by entity and time
query = (
    parsed_stream.writeStream
    .format("json")
    .outputMode("append")
    .partitionBy("entity", "year", "month", "day", "hour")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .queryName("rideflow_streaming_bronze")
    .start()
)

print(f"Streaming query started: {query.id}")
print(f"Status: {query.status}")

# The query runs continuously until the job is terminated.
# Monitor via: query.lastProgress, query.status
query.awaitTermination()
