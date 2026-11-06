# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Machine learning: churn prediction with MLflow and Unity Catalog
# MAGIC Question: *which customers who bought from us before 1 May will not come back in May and June?*
# MAGIC Features use only data **before** the cutoff; the label comes from **after** it. That avoids leakage.

# COMMAND ----------

import mlflow, pandas as pd
from pyspark.sql import functions as F
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, precision_score, recall_score

spark.sql("USE workspace.sakura")
CUTOFF = "2026-05-01"

# COMMAND ----------

# 1. Feature engineering in Spark (scales to millions of customers)
o = spark.table("silver_orders").filter("status = 'completed'")
i = spark.table("silver_order_items")
before = o.filter(F.col("order_date") < CUTOFF).join(i, "order_id")
after = o.filter(F.col("order_date") >= CUTOFF).select("customer_id").distinct().withColumn("came_back", F.lit(1))

features = (before.groupBy("customer_id").agg(
        F.datediff(F.lit(CUTOFF), F.max("order_date")).alias("recency_days"),
        F.countDistinct("order_id").alias("orders"),
        F.sum("amount").alias("spend_yen"),
        F.countDistinct("category").alias("categories"),
        F.avg(F.when(F.col("channel") == "app", 1).otherwise(0)).alias("app_share"),
        F.countDistinct(F.when(F.col("order_date") >= "2026-04-01", F.col("order_id"))).alias("orders_last_30d"))
    .join(spark.table("silver_customers").select("customer_id", "segment", "signup_date"), "customer_id")
    .withColumn("tenure_days", F.datediff(F.lit(CUTOFF), "signup_date"))
    .withColumn("is_premium", (F.col("segment") == "Premium").cast("int"))
    .join(after, "customer_id", "left")
    .withColumn("churned", F.when(F.col("came_back").isNull(), 1).otherwise(0))
    .drop("came_back", "signup_date", "segment"))

features.write.mode("overwrite").saveAsTable("churn_features")
display(features.groupBy("churned").count())

# COMMAND ----------

# 2. Train two models and track everything with MLflow
FEATS = ["recency_days", "orders", "spend_yen", "categories", "app_share", "orders_last_30d", "tenure_days", "is_premium"]
pdf = spark.table("churn_features").toPandas().sort_values("customer_id").reset_index(drop=True)
X_train, X_test, y_train, y_test = train_test_split(pdf[FEATS], pdf["churned"], test_size=0.25, random_state=42, stratify=pdf["churned"])

mlflow.set_experiment(f"/Users/{spark.sql('SELECT current_user()').first()[0]}/sakura-churn")
mlflow.sklearn.autolog(log_models=False)

candidates = {
    "logistic_regression": make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000)),
    "random_forest": RandomForestClassifier(n_estimators=300, min_samples_leaf=5, random_state=42),
}
results = {}
for name, model in candidates.items():
    with mlflow.start_run(run_name=name) as run:
        model.fit(X_train, y_train)
        p = model.predict_proba(X_test)[:, 1]
        m = dict(test_auc=roc_auc_score(y_test, p),
                 test_precision=precision_score(y_test, p > 0.5),
                 test_recall=recall_score(y_test, p > 0.5))
        mlflow.log_metrics(m)
        info = mlflow.sklearn.log_model(model, "model", input_example=X_train.head(3))
        results[name] = (info.model_uri, m)
        print(f"{name:20s} AUC={m['test_auc']:.3f}  precision={m['test_precision']:.2f}  recall={m['test_recall']:.2f}")

# COMMAND ----------

# 3. Register the best model in Unity Catalog and give it an alias
mlflow.set_registry_uri("databricks-uc")
best = max(results, key=lambda k: results[k][1]["test_auc"])
MODEL = "workspace.sakura.churn_model"
mv = mlflow.register_model(results[best][0], MODEL)
mlflow.MlflowClient().set_registered_model_alias(MODEL, "champion", mv.version)
print("registered", MODEL, "version", mv.version, "from", best)

# COMMAND ----------

# 4. Batch scoring: load the champion by its alias and score every customer.
#    (For millions of rows, use mlflow.pyfunc.spark_udf to score in parallel on the cluster.)
champion = mlflow.sklearn.load_model(f"models:/{MODEL}@champion")
pdf["churn_risk"] = champion.predict_proba(pdf[FEATS])[:, 1]
at_risk = (spark.createDataFrame(pdf[["customer_id", "recency_days", "orders", "spend_yen", "churn_risk"]])
           .join(spark.table("silver_customers").select("customer_id", "first_name", "city"), "customer_id")
           .orderBy(F.desc("churn_risk")))
at_risk.write.mode("overwrite").saveAsTable("churn_scores")
display(at_risk.filter("spend_yen > 30000").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Next steps**
# MAGIC * Serve the champion as a REST endpoint: *Serving > Create serving endpoint*, choose `workspace.sakura.churn_model`.
# MAGIC * Schedule this notebook weekly with Lakeflow Jobs (see `databricks.yml`) and send `churn_scores` to the CRM team.
