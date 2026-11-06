# The same clickstream pipeline as sakura_events.sql, written in Python.
# Use one or the other as the pipeline's source code (table names here end in _py so both can coexist).
from pyspark import pipelines as dp
from pyspark.sql import functions as F

VOL = "/Volumes/workspace/sakura/raw"


@dp.table(comment="Raw clickstream events, one row per event")
def events_bronze_py():
    return (spark.readStream.format("cloudFiles")       # Auto Loader; the pipeline manages schema and checkpoints
            .option("cloudFiles.format", "json")
            .load(f"{VOL}/events/")
            .withColumn("source_file", F.col("_metadata.file_path")))


@dp.table(comment="Clean clickstream events")
@dp.expect_or_drop("has_customer", "customer_id IS NOT NULL")
@dp.expect_or_drop("known_type", "event_type IN ('page_view', 'add_to_cart', 'purchase')")
@dp.expect("recent", "event_ts >= '2026-01-01'")                # warn only
def events_silver_py():
    return (spark.readStream.table("events_bronze_py")
            .select("event_id",
                    F.col("event_ts").cast("timestamp").alias("event_ts"),
                    F.col("customer_id").cast("int").alias("customer_id"),
                    "event_type",
                    F.col("product_id").cast("int").alias("product_id"),
                    "device"))


@dp.materialized_view(comment="Daily conversion funnel")
def funnel_daily_py():
    e = spark.read.table("events_silver_py")
    return (e.groupBy(F.to_date("event_ts").alias("day"), "device")
             .agg(F.count_if(F.col("event_type") == "page_view").alias("views"),
                  F.count_if(F.col("event_type") == "add_to_cart").alias("carts"),
                  F.count_if(F.col("event_type") == "purchase").alias("purchases")))
