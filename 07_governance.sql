-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 07 · Governance (Step 8)
-- MAGIC **8a: groups and grants.** Analysts can read gold, and nothing else.
-- MAGIC
-- MAGIC Before running: create the account group `finops_analysts` (see the chat for where).
-- MAGIC Run on serverless or a SQL warehouse. You own every object, so you keep full access throughout.

-- COMMAND ----------

-- Starting point: who has what on gold today (you, as owner, won't appear: owners need no grant)
SHOW GRANTS ON SCHEMA finops.gold;

-- COMMAND ----------

-- Analysts read gold only. All three are needed:
-- USE CATALOG opens finops, USE SCHEMA opens gold, SELECT reads the tables
GRANT USE CATALOG ON CATALOG finops TO `finops_analysts`;
GRANT USE SCHEMA ON SCHEMA finops.gold TO `finops_analysts`;
GRANT SELECT ON SCHEMA finops.gold TO `finops_analysts`;

-- COMMAND ----------

-- Check the catalog level
SHOW GRANTS `finops_analysts` ON CATALOG finops;

-- COMMAND ----------

-- Check the schema level
SHOW GRANTS `finops_analysts` ON SCHEMA finops.gold;

-- COMMAND ----------

-- SELECT on the schema reaches every table in it, including ones the pipeline adds later.
-- Look at the object type/key columns: the grant comes from the schema, not the table.
SHOW GRANTS `finops_analysts` ON TABLE finops.gold.copilot_user_monthly;

-- COMMAND ----------

-- Silver holds raw usernames and has no grant: expect no rows here
SHOW GRANTS `finops_analysts` ON SCHEMA finops.silver;

-- COMMAND ----------

-- REVOKE takes a grant away. Remove USE SCHEMA and check: SELECT is still listed,
-- but without USE SCHEMA the group can no longer reach any gold table
REVOKE USE SCHEMA ON SCHEMA finops.gold FROM `finops_analysts`;
SHOW GRANTS `finops_analysts` ON SCHEMA finops.gold;

-- COMMAND ----------

-- Put it back
GRANT USE SCHEMA ON SCHEMA finops.gold TO `finops_analysts`;
SHOW GRANTS `finops_analysts` ON SCHEMA finops.gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **8b: column mask on usernames.** Before running: create the account group `finops_admins` (leave it empty for now).

-- COMMAND ----------

-- A schema for governance objects, kept apart from the data layers
CREATE SCHEMA IF NOT EXISTS finops.governance;

-- COMMAND ----------

-- The mask: finops_admins see the real name, everyone else sees a stable code.
-- Same user -> same code, so per-user counts and charts still work.
CREATE OR REPLACE FUNCTION finops.governance.mask_username(username STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('finops_admins') THEN username
  ELSE concat('user_', substr(sha2(username, 256), 1, 8))
END;

-- COMMAND ----------

-- Run AFTER the pipeline update. You're not in finops_admins yet, so expect user_xxxxxxxx codes,
-- even though you own the table: masks apply to owners too
SELECT usage_month, username, credits_used
FROM finops.gold.copilot_user_monthly
ORDER BY credits_used DESC
LIMIT 5;

-- COMMAND ----------

-- Where the mask is attached: look for the mask on the username column
DESCRIBE TABLE EXTENDED finops.gold.copilot_user_monthly;
