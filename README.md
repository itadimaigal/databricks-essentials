# Databricks Essentials: hands-on examples

Companion repository for the **itadimaigal · Architect Talks** video *Databricks Essentials*, presented by Professor SK.

Follow a small online shop, **Sakura Mart**, whose revenue dropped 30% in April. Three people find out why on one platform:

* **Kenji, data engineer:** ingests messy CSV and JSON files, builds a bronze/silver/gold pipeline, and adds data-quality rules and a daily job.
* **Aya, data analyst:** writes SQL, builds a dashboard and asks Genie questions.
* **Hiro, ML engineer:** trains a churn model with MLflow, registers it in Unity Catalog and scores customers.

Everything runs on **Databricks Free Edition** (serverless). The data is synthetic, and every person and company in it is fictional.

## What's inside

| Path | What it is |
|---|---|
| `data/` | Sakura Mart sample data: customers, products, orders, order items, an updates batch, clickstream events and support tickets |
| `notebooks/00_setup.py` | Creates the `workspace.sakura` schema and a `raw` volume. Upload `data/` here |
| `notebooks/01_spark_basics.py` | DataFrames, lazy evaluation, joins, aggregates and SQL |
| `notebooks/02_delta_lake.py` | Delta tables: UPDATE, MERGE, DELETE, history, time travel, schema enforcement, RESTORE and liquid clustering |
| `notebooks/03_medallion_pipeline.py` | Bronze, silver and gold tables with a quarantine table |
| `notebooks/04_streaming_autoloader.py` | Incremental ingestion with Auto Loader and `trigger(availableNow=True)` |
| `notebooks/05_sql_analytics.sql` | Top products, month-over-month growth, revenue by city, cohort retention and RFM |
| `notebooks/06_governance.sql` | Grants, column masks, row filters, tags and lineage |
| `notebooks/07_ml_churn_mlflow.py` | Churn features, MLflow tracking, Unity Catalog model registry, champion alias and batch scoring |
| `notebooks/08_ai_functions.sql` | `ai_analyze_sentiment`, `ai_classify`, `ai_extract` and `ai_query` on support tickets |
| `pipelines/sakura_events.sql` | Lakeflow Spark Declarative Pipeline (formerly DLT) with expectations |
| `pipelines/sakura_events_python.py` | The same pipeline in Python (`from pyspark import pipelines as dp`) |
| `databricks.yml` | Bundle that deploys the pipeline and a daily job |
| `serving/` | Example REST call to a model serving endpoint |
| `local/reproduce_results.py` | Reproduces the video's results on your laptop with pandas, SQLite and scikit-learn |
| `expected_output/` | The results shown in the video, to compare against your own |

## Quick start

1. Sign up for [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition).
2. In your workspace, go to **Workspace > Create > Git folder** and paste this repository's URL. You can also import the notebooks one by one.
3. Open `notebooks/00_setup.py` and run the first cell. It creates the schema and the volume.
4. In **Catalog**, open the volume `workspace.sakura.raw` and click **Upload to this volume**.
   * Upload every file from `data/`.
   * Put the three `events_*.json` files in a sub-folder called `events_all/`.
5. Run the notebooks in order: `01` → `08`.
   * `05` and `06` are SQL notebooks. They also run in the SQL editor.
   * `08` needs AI Functions to be available in your workspace.

### The pipeline and the job

**In the UI:** go to **Jobs & Pipelines > Create > ETL pipeline**.

1. Add `pipelines/sakura_events.sql` as the source code.
2. Set the target to catalog `workspace` and schema `sakura`.
3. Click **Start**.

**As code,** with the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/):

```bash
databricks auth login --host https://<your-workspace-url>
databricks bundle validate
databricks bundle deploy
databricks bundle run sakura_daily
```

Change `you@example.com` in `databricks.yml` to your own email first.

## Check your results without Databricks

```bash
pip install pandas scikit-learn
python local/reproduce_results.py
```

Counts and totals should match the Databricks notebooks exactly. Two kinds of output can differ:

* **Machine learning metrics** can move slightly with library versions.
* **AI Function outputs** come from a language model and vary between runs. The ones in the video are examples.

## Notes

* **Catalog name:** Free Edition's default catalog is `workspace`. If yours is different, change `CATALOG` and the `/Volumes/workspace/...` paths.
* **Groups:** the governance notebook uses example groups (`analysts`, `pii_readers`, `osaka_team`, `admins`). Create them or use your own.
* **AI endpoint:** the `ai_query` endpoint name is an example. Pick one from the **Serving** page of your workspace.
* **Product names:** Databricks renames products from time to time. For example, DLT became Lakeflow Spark Declarative Pipelines, and Databricks Asset Bundles became Declarative Automation Bundles. If a menu looks different, check the [documentation](https://docs.databricks.com/).

Made for the itadimaigal YouTube channel. Use, change and share freely.
