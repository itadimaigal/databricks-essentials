-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 08 · Generative AI in SQL: AI Functions
-- MAGIC Built-in SQL functions call a model hosted by Databricks. Availability depends on your region and workspace;
-- MAGIC the model's wording can differ from run to run. Requires a recent runtime or a SQL warehouse (not classic).

-- COMMAND ----------

USE workspace.sakura;

CREATE OR REPLACE TABLE support_tickets AS
SELECT * FROM read_files('/Volumes/workspace/sakura/raw/support_tickets.csv', format => 'csv', header => true);

-- COMMAND ----------

-- 1. Sentiment and category, one row per ticket
SELECT ticket_id,
       message,
       ai_analyze_sentiment(message) AS sentiment,
       ai_classify(message, ARRAY('damaged or wrong item', 'billing', 'delivery', 'product defect',
                                  'app or account', 'order change', 'praise', 'question')) AS category
FROM support_tickets;

-- COMMAND ----------

-- 2. Pull structured fields out of free text
SELECT ticket_id,
       ai_extract(message, ARRAY('product', 'order_number', 'place')) AS extracted
FROM support_tickets
WHERE ticket_id IN (1, 3, 6);

-- COMMAND ----------

-- 3. Your own prompt with ai_query. Pick an endpoint name from the Serving page of your workspace.
SELECT ticket_id,
       ai_query('databricks-claude-sonnet-4-5',
                CONCAT('You are a polite customer support agent for Sakura Mart, an online shop in Japan. ',
                       'Write a two-sentence reply to this message: ', message)) AS draft_reply
FROM support_tickets
WHERE ticket_id = 1;

-- COMMAND ----------

-- 4. Put it together: an AI-enriched ticket table that a Lakeflow Job refreshes every hour
CREATE OR REPLACE TABLE support_tickets_enriched AS
SELECT t.*,
       ai_analyze_sentiment(message) AS sentiment,
       ai_classify(message, ARRAY('damaged or wrong item', 'billing', 'delivery', 'product defect',
                                  'app or account', 'order change', 'praise', 'question')) AS category,
       c.segment
FROM support_tickets t LEFT JOIN silver_customers c USING (customer_id);

SELECT category, COUNT(*) AS tickets, COUNT_IF(sentiment = 'negative') AS negative
FROM support_tickets_enriched GROUP BY category ORDER BY tickets DESC;
