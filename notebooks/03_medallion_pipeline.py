# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Medallion pipeline: bronze → silver → gold
# MAGIC Bronze keeps the raw data exactly as it arrived, silver is clean and typed, gold is ready for the business.

# COMMAND ----------

from pyspark.sql import functions as F, Window
spark.sql("USE workspace.sakura")
VOL = "/Volumes/workspace/sakura/raw"

def read_raw(name, schema=None):
    r = spark.read.option("header", True)
    r = r.schema(schema) if schema else r.option("inferSchema", True)
    return (r.csv(f"{VOL}/{name}.csv")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp()))

# COMMAND ----------

# BRONZE: raw, append-only, plus where/when it came from
read_raw("orders", "order_id INT, customer_id INT, order_ts TIMESTAMP, status STRING, channel STRING") \
    .write.mode("overwrite").saveAsTable("bronze_orders")
read_raw("order_items").write.mode("overwrite").saveAsTable("bronze_order_items")
read_raw("customers").write.mode("overwrite").saveAsTable("bronze_customers")
read_raw("products").write.mode("overwrite").saveAsTable("bronze_products")

# COMMAND ----------

# SILVER: de-duplicate, validate, conform
silver_orders = (spark.table("bronze_orders")
    .dropDuplicates(["order_id"])
    .filter(F.col("customer_id").isNotNull())
    .withColumn("order_date", F.to_date("order_ts"))
    .drop("_source_file", "_ingested_at"))

bad_items = spark.table("bronze_order_items").filter("quantity <= 0 OR unit_price <= 0")
silver_items = (spark.table("bronze_order_items")
    .filter("quantity > 0 AND unit_price > 0")
    .join(spark.table("bronze_products").select("product_id", "product_name", "category"), "product_id")
    .withColumn("amount", F.col("quantity") * F.col("unit_price"))
    .drop("_source_file", "_ingested_at"))

silver_orders.write.mode("overwrite").saveAsTable("silver_orders")
silver_items.write.mode("overwrite").saveAsTable("silver_order_items")
bad_items.write.mode("overwrite").saveAsTable("quarantine_order_items")
spark.table("bronze_customers").drop("_source_file", "_ingested_at").write.mode("overwrite").saveAsTable("silver_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze_orders' AS table_name, COUNT(*) AS row_count FROM bronze_orders UNION ALL
# MAGIC SELECT 'silver_orders', COUNT(*) FROM silver_orders UNION ALL
# MAGIC SELECT 'bronze_order_items', COUNT(*) FROM bronze_order_items UNION ALL
# MAGIC SELECT 'silver_order_items', COUNT(*) FROM silver_order_items UNION ALL
# MAGIC SELECT 'quarantine_order_items', COUNT(*) FROM quarantine_order_items

# COMMAND ----------

# GOLD 1: daily sales by category (completed orders only)
completed = spark.table("silver_orders").filter("status = 'completed'")
gold_daily = (completed.join(spark.table("silver_order_items"), "order_id")
    .groupBy("order_date", "category")
    .agg(F.sum("amount").alias("revenue_yen"), F.countDistinct("order_id").alias("orders")))
gold_daily.write.mode("overwrite").saveAsTable("gold_daily_sales")

# COMMAND ----------

# GOLD 2: one row per customer (customer 360)
lines = completed.join(spark.table("silver_order_items"), "order_id")
fav = (lines.groupBy("customer_id", "category").agg(F.sum("amount").alias("spend"))
       .withColumn("rk", F.row_number().over(Window.partitionBy("customer_id").orderBy(F.desc("spend"), "category")))
       .filter("rk = 1").select("customer_id", F.col("category").alias("favourite_category")))
c360 = (lines.groupBy("customer_id")
        .agg(F.countDistinct("order_id").alias("orders"),
             F.sum("amount").alias("lifetime_value_yen"),
             F.min("order_date").alias("first_order"),
             F.max("order_date").alias("last_order"))
        .join(fav, "customer_id")
        .join(spark.table("silver_customers").select("customer_id", "city", "segment"), "customer_id"))
c360.write.mode("overwrite").saveAsTable("gold_customer_360")
display(spark.table("gold_customer_360").orderBy(F.desc("lifetime_value_yen")).limit(5))
