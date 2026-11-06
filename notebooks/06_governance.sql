-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 · Governance with Unity Catalog
-- MAGIC Permissions, column masks, row filters, tags and lineage. Group names (`analysts`, `pii_readers`, `osaka_team`)
-- MAGIC are examples: create them in your workspace, or replace them with groups you already have.
-- MAGIC In Free Edition you are the only user, which is perfect for testing: the mask below will hide emails from you.

-- COMMAND ----------

USE workspace.sakura;

-- COMMAND ----------

-- 1. Permissions follow the three-level namespace: catalog > schema > table
GRANT USE CATALOG ON CATALOG workspace TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA workspace.sakura TO `analysts`;
SHOW GRANTS ON SCHEMA workspace.sakura;

-- COMMAND ----------

-- 2. Column mask: only members of pii_readers see real email addresses
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURN CASE WHEN is_account_group_member('pii_readers') THEN email
            ELSE CONCAT(LEFT(email, 2), '***@', SPLIT_PART(email, '@', 2)) END;

-- (We mask a copy, so the daily job can keep overwriting silver_customers.)
CREATE OR REPLACE TABLE customers_pii AS SELECT * FROM silver_customers;
ALTER TABLE customers_pii ALTER COLUMN email SET MASK mask_email;

SELECT customer_id, first_name, email, city FROM customers_pii ORDER BY customer_id LIMIT 4;

-- COMMAND ----------

-- 3. Row filter: the Osaka team only sees Osaka customers; admins see everything
CREATE OR REPLACE FUNCTION city_filter(city STRING)
RETURN is_account_group_member('admins') OR (is_account_group_member('osaka_team') AND city = 'Osaka');

CREATE OR REPLACE TABLE customers_by_region AS SELECT * FROM gold_customer_360;
ALTER TABLE customers_by_region SET ROW FILTER city_filter ON (city);

-- COMMAND ----------

-- 4. Tags and comments make data discoverable (and searchable in Catalog Explorer)
ALTER TABLE customers_pii SET TAGS ('domain' = 'sales', 'contains_pii' = 'true');
COMMENT ON TABLE gold_customer_360 IS 'One row per customer with lifetime value and favourite category. Owner: data-eng.';

-- COMMAND ----------

-- 5. Lineage is captured automatically. Open gold_customer_360 in Catalog Explorer > Lineage,
--    or query the system tables (where system tables are enabled):
SELECT source_table_full_name, target_table_full_name, MAX(event_time) AS last_seen
FROM system.access.table_lineage
WHERE target_table_full_name = 'workspace.sakura.gold_customer_360'
GROUP BY ALL;

-- COMMAND ----------

-- To undo:
-- ALTER TABLE customers_pii ALTER COLUMN email DROP MASK;
-- ALTER TABLE customers_by_region DROP ROW FILTER;
