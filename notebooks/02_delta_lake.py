# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Delta Lake: ACID tables, MERGE, time travel, schema enforcement

# COMMAND ----------

from pyspark.sql import functions as F
spark.sql("USE workspace.sakura")
VOL = "/Volumes/workspace/sakura/raw"
SCHEMA_ORDERS = "order_id INT, customer_id INT, order_ts TIMESTAMP, status STRING, channel STRING"

orders = spark.read.csv(f"{VOL}/orders.csv", header=True, schema=SCHEMA_ORDERS)

# Write a managed Delta table (version 0)
orders.dropDuplicates(["order_id"]).write.mode("overwrite").saveAsTable("orders_delta")
print(spark.table("orders_delta").count())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A customer called to cancel one order (version 1)
# MAGIC UPDATE orders_delta SET status = 'cancelled' WHERE order_id = 506319

# COMMAND ----------

# Upsert the 1 July batch: 50 status changes + 20 new orders (version 2)
updates = spark.read.csv(f"{VOL}/orders_updates.csv", header=True, schema=SCHEMA_ORDERS)
updates.createOrReplaceTempView("order_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO orders_delta AS t
# MAGIC USING order_updates AS s
# MAGIC ON t.order_id = s.order_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Housekeeping: remove old cancelled orders (version 3)
# MAGIC DELETE FROM orders_delta WHERE status = 'cancelled' AND order_ts < '2026-02-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time travel: compare today with version 0
# MAGIC SELECT 'v0' AS version, COUNT(*) AS orders, COUNT_IF(status = 'returned') AS returned FROM orders_delta VERSION AS OF 0
# MAGIC UNION ALL
# MAGIC SELECT 'now', COUNT(*), COUNT_IF(status = 'returned') FROM orders_delta

# COMMAND ----------

# Schema enforcement: a new column appears in the source...
with_coupon = updates.limit(3).withColumn("coupon_code", F.lit("SPRING10"))
try:
    with_coupon.write.mode("append").saveAsTable("orders_delta")
except Exception as e:
    print(str(e)[:300])

# COMMAND ----------

# ...and schema evolution when you really want it (version 4)
with_coupon.write.mode("append").option("mergeSchema", "true").saveAsTable("orders_delta")
spark.table("orders_delta").printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Oops, those 3 rows were a test. Roll the table back (version 5)
# MAGIC RESTORE TABLE orders_delta TO VERSION AS OF 3

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Layout: liquid clustering on the column we filter by most, then compact files
# MAGIC ALTER TABLE orders_delta CLUSTER BY (customer_id);
# MAGIC OPTIMIZE orders_delta;
