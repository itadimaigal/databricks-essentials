# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Spark basics: read, explore, transform
# MAGIC DataFrames in Python and the same questions in SQL. Run `00_setup` first.

# COMMAND ----------

from pyspark.sql import functions as F

VOL = "/Volumes/workspace/sakura/raw"

orders = (spark.read.format("csv")
          .option("header", True)
          .schema("order_id INT, customer_id INT, order_ts TIMESTAMP, status STRING, channel STRING")
          .load(f"{VOL}/orders.csv"))

orders.printSchema()

# COMMAND ----------

display(orders.limit(5))
print("rows:", orders.count())

# COMMAND ----------

# Transformations are lazy: nothing runs until an action such as display(), count() or write
by_status = (orders.groupBy("status")
             .agg(F.count("*").alias("orders"))
             .orderBy(F.desc("orders")))
display(by_status)

# COMMAND ----------

items = spark.read.csv(f"{VOL}/order_items.csv", header=True, inferSchema=True)
products = spark.read.csv(f"{VOL}/products.csv", header=True, inferSchema=True)

revenue_by_category = (items
    .join(products.select("product_id", "category"), "product_id")
    .withColumn("amount", F.col("quantity") * F.col("unit_price"))
    .groupBy("category")
    .agg(F.sum("amount").alias("revenue_yen"),
         F.countDistinct("order_id").alias("orders"))
    .orderBy(F.desc("revenue_yen")))
display(revenue_by_category)

# COMMAND ----------

# The same data, from SQL: register a temporary view
orders.createOrReplaceTempView("orders_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT channel,
# MAGIC        COUNT(*)                                   AS orders,
# MAGIC        ROUND(AVG(HOUR(order_ts)), 1)              AS avg_hour,
# MAGIC        COUNT_IF(status = 'cancelled')             AS cancelled
# MAGIC FROM orders_v
# MAGIC GROUP BY channel
# MAGIC ORDER BY orders DESC

# COMMAND ----------

# A quick data-quality check: is order_id really unique?
dupes = orders.groupBy("order_id").count().filter("count > 1")
print("duplicated order_ids:", dupes.count())
