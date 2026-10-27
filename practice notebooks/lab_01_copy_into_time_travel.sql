-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lab 1 · COPY INTO remembers files, Delta remembers versions
-- MAGIC **Destructive:** run one cell at a time, not "Run all". It deletes every row in a bronze table you can rebuild from the volume.
-- MAGIC
-- MAGIC 1. Look at the version history
-- MAGIC 2. Delete every row (the "accident"), then run `02_bronze_copy_into`: it loads 0 rows, because COPY INTO tracks files, not rows
-- MAGIC 3. Find the version before the DELETE and restore it
-- MAGIC 4. See that RESTORE added a new version instead of erasing history
-- MAGIC
-- MAGIC **Exam:** Section 1 (Delta Lake time travel), Section 2 (COPY INTO load tracking)

-- COMMAND ----------

-- Every write to a Delta table creates a new version
DESCRIBE HISTORY finops.bronze.focus_costs;

-- COMMAND ----------

-- The "accident". Afterwards, run 02_bronze_copy_into and watch it load 0 rows
DELETE FROM finops.bronze.focus_costs;

-- COMMAND ----------

-- Find the DELETE row; the version just below it is the last good one
DESCRIBE HISTORY finops.bronze.focus_costs;

-- COMMAND ----------

-- Rewind (edit the number to the version you found above)
RESTORE TABLE finops.bronze.focus_costs TO VERSION AS OF 1;

-- COMMAND ----------

-- RESTORE added a new version; the DELETE is still in the history (the audit trail)
DESCRIBE HISTORY finops.bronze.focus_costs;
