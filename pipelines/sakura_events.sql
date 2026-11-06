-- Lakeflow Spark Declarative Pipeline (formerly DLT) for the clickstream.
-- Create it in the UI: Jobs & Pipelines > Create > ETL pipeline, add this file as the source code,
-- target catalog = workspace, schema = sakura. Or deploy it with the bundle in databricks.yml.

-- 1. Bronze: incrementally read every new JSON file from the landing folder
CREATE OR REFRESH STREAMING TABLE events_bronze
COMMENT 'Raw clickstream events, one row per event'
AS SELECT *, _metadata.file_path AS source_file
FROM STREAM read_files('/Volumes/workspace/sakura/raw/events/', format => 'json');

-- 2. Silver: typed and validated. Bad rows are dropped and counted in the pipeline's data quality metrics
CREATE OR REFRESH STREAMING TABLE events_silver (
  CONSTRAINT has_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT known_type   EXPECT (event_type IN ('page_view', 'add_to_cart', 'purchase')) ON VIOLATION DROP ROW,
  CONSTRAINT recent       EXPECT (event_ts >= '2026-01-01')
)
COMMENT 'Clean clickstream events'
AS SELECT event_id,
          CAST(event_ts AS TIMESTAMP) AS event_ts,
          CAST(customer_id AS INT)    AS customer_id,
          event_type,
          CAST(product_id AS INT)     AS product_id,
          device
FROM STREAM events_bronze;

-- 3. Gold: a materialized view the dashboard reads. Refreshed by the pipeline, incrementally when possible
CREATE OR REFRESH MATERIALIZED VIEW funnel_daily
COMMENT 'Daily conversion funnel'
AS SELECT DATE(event_ts) AS day,
          device,
          COUNT_IF(event_type = 'page_view')   AS views,
          COUNT_IF(event_type = 'add_to_cart') AS carts,
          COUNT_IF(event_type = 'purchase')    AS purchases
FROM events_silver
GROUP BY DATE(event_ts), device;
