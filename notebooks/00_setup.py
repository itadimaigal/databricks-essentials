# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup: schema, volume and sample data
# MAGIC Run this once. It creates a schema and a volume in Unity Catalog for the Sakura Mart sample data.
# MAGIC
# MAGIC **Before you run it:** in the Catalog explorer, open the volume `workspace.sakura.raw` (created by the cell below)
# MAGIC and use **Upload to this volume** to upload every file from the repo's `data/` folder.
# MAGIC Put the three `events_*.json` files into a sub-folder called `events_all/`.

# COMMAND ----------

CATALOG = "workspace"      # Free Edition's default catalog. Change it if your workspace uses another one.
SCHEMA = "sakura"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

# After uploading, check the files are there
display(dbutils.fs.ls(VOL))

# COMMAND ----------

# The streaming notebook simulates files arriving one by one, so it needs an empty landing folder
dbutils.fs.mkdirs(f"{VOL}/events")
display(dbutils.fs.ls(f"{VOL}/events_all"))
