# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Pipeline: silver FOCUS costs
# MAGIC Second source file for `finops_cost_pipeline`. Reads `focus_raw` (bronze), fixes types, keeps only the latest export run per billing period.
# MAGIC
# MAGIC **Before the first run:** `CREATE SCHEMA IF NOT EXISTS finops.silver;`

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# Materialized view: a re-sent month must REPLACE the old rows, which a streaming table can't do
@dp.materialized_view(name="finops.silver.focus_costs",
                      comment="FOCUS cost rows with proper types; latest export run per billing period only")
def focus_costs():
    bronze = (spark.read.table("focus_raw")
        # Path looks like .../george-test-focus-cost/20261001-20261031/<run id>/part_0_0001.csv.gz
        .withColumn("billing_period", F.regexp_extract("source_file", r"/(\d{8}-\d{8})/[^/]+/[^/]+$", 1))
        .withColumn("run_id",         F.regexp_extract("source_file", r"/\d{8}-\d{8}/([^/]+)/[^/]+$", 1)))

    # One row per export run: when its files were written
    runs = (bronze.groupBy("billing_period", "run_id")
            .agg(F.max("source_modified_at").alias("run_time")))

    # Number runs newest-first within each billing period; keep number 1
    newest_first = Window.partitionBy("billing_period").orderBy(F.desc("run_time"))
    latest_runs = (runs.withColumn("rn", F.row_number().over(newest_first))
                       .filter("rn = 1")
                       .select("billing_period", "run_id"))

    # Inner join drops rows from older runs of the same month
    latest = bronze.join(latest_runs, ["billing_period", "run_id"])

    # Bronze is all strings; try_cast gives NULL (instead of failing) on a bad value
    return latest.withColumns({
        "BilledCost":         F.expr("try_cast(BilledCost AS DECIMAL(18,6))"),
        "EffectiveCost":      F.expr("try_cast(EffectiveCost AS DECIMAL(18,6))"),
        "ListCost":           F.expr("try_cast(ListCost AS DECIMAL(18,6))"),
        "ConsumedQuantity":   F.expr("try_cast(ConsumedQuantity AS DECIMAL(18,6))"),
        "ChargePeriodStart":  F.expr("try_cast(ChargePeriodStart AS TIMESTAMP)"),
        "ChargePeriodEnd":    F.expr("try_cast(ChargePeriodEnd AS TIMESTAMP)"),
        "BillingPeriodStart": F.expr("try_cast(BillingPeriodStart AS TIMESTAMP)"),
        "BillingPeriodEnd":   F.expr("try_cast(BillingPeriodEnd AS TIMESTAMP)"),
    }).drop("_rescued_data")
