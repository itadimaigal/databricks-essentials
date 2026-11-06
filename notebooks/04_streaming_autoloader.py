# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Incremental ingestion with Auto Loader
# MAGIC New clickstream files land in a volume. Auto Loader picks up only the files it has not seen before.

# COMMAND ----------

from pyspark.sql import functions as F
spark.sql("USE workspace.sakura")
VOL = "/Volumes/workspace/sakura/raw"

def land(day):
    """Simulate a new file arriving in the landing folder."""
    dbutils.fs.cp(f"{VOL}/events_all/events_{day}.json", f"{VOL}/events/events_{day}.json")

land("2026-07-01"); land("2026-07-02")

# COMMAND ----------

def table_rows():
    return spark.table("bronze_events").count() if spark.catalog.tableExists("bronze_events") else 0

def ingest():
    before = table_rows()
    stream = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", f"{VOL}/_schemas/events")
              .option("cloudFiles.inferColumnTypes", "true")
              .load(f"{VOL}/events/")
              .withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_ingested_at", F.current_timestamp()))
    q = (stream.writeStream
         .option("checkpointLocation", f"{VOL}/_checkpoints/events")
         .trigger(availableNow=True)          # process what is new, then stop
         .toTable("bronze_events"))
    q.awaitTermination()
    return table_rows() - before

print("new rows:", ingest())
print("table rows:", spark.table("bronze_events").count())

# COMMAND ----------

# A day later, one more file arrives. Run the same code again.
land("2026-07-03")
print("new rows:", ingest())
print("table rows:", spark.table("bronze_events").count())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Conversion funnel per day
# MAGIC SELECT DATE(event_ts)                    AS day,
# MAGIC        COUNT_IF(event_type = 'page_view')   AS views,
# MAGIC        COUNT_IF(event_type = 'add_to_cart') AS carts,
# MAGIC        COUNT_IF(event_type = 'purchase')    AS purchases,
# MAGIC        ROUND(100 * COUNT_IF(event_type = 'purchase') / COUNT_IF(event_type = 'page_view'), 1) AS conversion_pct
# MAGIC FROM bronze_events
# MAGIC GROUP BY 1 ORDER BY 1
