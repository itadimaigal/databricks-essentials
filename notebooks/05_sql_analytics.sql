-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · SQL analytics for data analysts
-- MAGIC Run on a SQL warehouse (SQL editor) or in a notebook. Uses the silver and gold tables from notebook 03.

-- COMMAND ----------

USE workspace.sakura;

-- COMMAND ----------

-- Q1. Top 5 products by revenue (completed orders)
SELECT i.product_name,
       i.category,
       SUM(i.quantity)            AS units,
       SUM(i.amount)              AS revenue_yen
FROM silver_order_items i
JOIN silver_orders o USING (order_id)
WHERE o.status = 'completed'
GROUP BY i.product_name, i.category
ORDER BY revenue_yen DESC
LIMIT 5;

-- COMMAND ----------

-- Q2. Monthly revenue and month-over-month growth (window function LAG)
WITH monthly AS (
  SELECT DATE_TRUNC('MONTH', order_date) AS month,
         SUM(revenue_yen)               AS revenue_yen
  FROM gold_daily_sales
  GROUP BY 1
)
SELECT DATE_FORMAT(month, 'yyyy-MM')                                   AS month,
       revenue_yen,
       ROUND(100 * (revenue_yen / LAG(revenue_yen) OVER (ORDER BY month) - 1), 1) AS mom_growth_pct
FROM monthly
ORDER BY month;

-- COMMAND ----------

-- Q3. Revenue by city and channel, for the dashboard
SELECT c.city,
       ROUND(SUM(CASE WHEN o.channel = 'app' THEN i.amount END) / 1e6, 2) AS app_m_yen,
       ROUND(SUM(CASE WHEN o.channel = 'web' THEN i.amount END) / 1e6, 2) AS web_m_yen,
       ROUND(SUM(i.amount) / 1e6, 2)                                       AS total_m_yen
FROM silver_orders o
JOIN silver_order_items i USING (order_id)
JOIN silver_customers c USING (customer_id)
WHERE o.status = 'completed'
GROUP BY c.city
ORDER BY total_m_yen DESC;

-- COMMAND ----------

-- Q4. Cohort retention: of the customers whose first order was in month M, how many ordered again in later months?
WITH firsts AS (
  SELECT customer_id, DATE_TRUNC('MONTH', MIN(order_date)) AS cohort
  FROM silver_orders WHERE status = 'completed' GROUP BY customer_id
),
activity AS (
  SELECT DISTINCT o.customer_id, f.cohort,
         MONTHS_BETWEEN(DATE_TRUNC('MONTH', o.order_date), f.cohort) AS month_n
  FROM silver_orders o JOIN firsts f USING (customer_id)
  WHERE o.status = 'completed'
)
SELECT DATE_FORMAT(cohort, 'yyyy-MM') AS cohort,
       COUNT_IF(month_n = 0) AS customers,
       ROUND(100 * COUNT_IF(month_n = 1) / COUNT_IF(month_n = 0)) AS m1_pct,
       ROUND(100 * COUNT_IF(month_n = 2) / COUNT_IF(month_n = 0)) AS m2_pct,
       ROUND(100 * COUNT_IF(month_n = 3) / COUNT_IF(month_n = 0)) AS m3_pct
FROM activity
GROUP BY cohort
ORDER BY cohort;

-- COMMAND ----------

-- Q5. RFM segmentation with NTILE (recency, frequency, monetary)
WITH rfm AS (
  SELECT customer_id,
         DATEDIFF(DATE'2026-06-30', last_order) AS recency_days,
         orders                                 AS frequency,
         lifetime_value_yen                     AS monetary
  FROM gold_customer_360
),
scored AS (
  SELECT *,
         NTILE(4) OVER (ORDER BY recency_days DESC, customer_id) AS r,
         NTILE(4) OVER (ORDER BY frequency, customer_id)         AS f,
         NTILE(4) OVER (ORDER BY monetary, customer_id)          AS m
  FROM rfm
)
SELECT CASE WHEN r = 4 AND f >= 3 THEN 'Champions'
            WHEN r >= 3 AND f >= 2 THEN 'Loyal'
            WHEN r <= 2 AND f >= 3 THEN 'At risk'
            WHEN r = 1             THEN 'Hibernating'
            ELSE 'Needs attention' END AS segment,
       COUNT(*)                       AS customers,
       ROUND(AVG(monetary))           AS avg_value_yen,
       ROUND(AVG(recency_days))       AS avg_recency_days
FROM scored
GROUP BY 1
ORDER BY avg_value_yen DESC;
