# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Pipeline: silver FOCUS costs
# MAGIC Second source file for `finops_cost_pipeline`. Reads `focus_raw` (bronze), fixes types, keeps only the latest export run per billing period.
# MAGIC
# MAGIC **Step 6c:** expectations check every silver row: warn (keep + count), drop (remove), fail (stop the update).
# MAGIC A second view, `focus_load_check`, checks that the run we kept has exactly the rows its manifest says Azure wrote.
# MAGIC
# MAGIC **Before the first run:** `CREATE SCHEMA IF NOT EXISTS finops.silver;`

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# Data quality rules for silver rows
WARN_RULES = {
    "billed_cost_parsed":    "BilledCost IS NOT NULL",       # try_cast gave NULL: bad number in the file
    "effective_cost_parsed": "EffectiveCost IS NOT NULL",
    "non_negative_billed_cost": "BilledCost >= 0",           # $0 free-tier rows are normal; flag refunds/credits or errors
    "billing_period_found":  "billing_period != ''",         # path pattern changed: regex found no period folder
}

# COMMAND ----------

# Adds billing_period and run_id from the file path, then keeps only the newest export run per billing period
def latest_run(df):
    # Path looks like .../<export name>/20261001-20261031/<run id>/part_0_0001.csv.gz
    df = (df.withColumn("billing_period", F.regexp_extract("source_file", r"/(\d{8}-\d{8})/[^/]+/[^/]+$", 1))
            .withColumn("run_id",         F.regexp_extract("source_file", r"/\d{8}-\d{8}/([^/]+)/[^/]+$", 1)))

    # One row per export run: when its files were written
    runs = df.groupBy("billing_period", "run_id").agg(F.max("source_modified_at").alias("run_time"))

    # Number runs newest-first within each billing period; keep number 1
    newest_first = Window.partitionBy("billing_period").orderBy(F.desc("run_time"))
    latest = (runs.withColumn("rn", F.row_number().over(newest_first))
                  .filter("rn = 1")
                  .select("billing_period", "run_id"))

    # Inner join drops rows from older runs of the same month
    return df.join(latest, ["billing_period", "run_id"])

# COMMAND ----------

# Materialized view: a re-sent month must REPLACE the old rows, which a streaming table can't do
@dp.expect_all(WARN_RULES)                                                   # warn: keep the row, count it
@dp.expect_or_drop("has_charge_period", "ChargePeriodStart IS NOT NULL")     # drop: a cost with no date can't go in a daily total
@dp.expect_or_fail("single_currency", "BillingCurrency = 'USD'")             # fail: totals assume one currency, so stop
@dp.materialized_view(name="finops.silver.focus_costs",
                      comment="FOCUS cost rows with proper types; latest export run per billing period only")
def focus_costs():
    latest = latest_run(spark.read.table("focus_raw"))

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

# COMMAND ----------

# Each FOCUS run's manifest.json: Azure's own record of the run and how many rows it wrote
# Path: exports/george-test-focus-cost/<billing period>/<run id>/manifest.json
FOCUS_MANIFESTS = "/Volumes/finops/raw/real_exports/exports/george-test-focus-cost/*/*/manifest.json"

def focus_manifests():
    return (spark.read.option("multiLine", True)          # each manifest is one JSON document over many lines
            .json(FOCUS_MANIFESTS)
            .select(F.col("runInfo.runId").alias("run_id"),
                    F.col("dataRowCount").alias("manifest_rows"),
                    F.to_date("runInfo.endDate").alias("covers_until")))

# One row per billing period: the run we kept, its rows, and the manifest's count for that run
# Counted in bronze (before silver drops bad rows), so it checks loading + picking the newest run
@dp.expect_or_fail("manifest_found", "manifest_rows IS NOT NULL")         # fail: data with no manifest = run still being written, or files missing
@dp.expect_or_fail("rows_match_manifest", "loaded_rows = manifest_rows")  # fail: we loaded more or fewer rows than Azure wrote
@dp.materialized_view(name="finops.silver.focus_load_check",
                      comment="Per billing period: rows loaded from the newest FOCUS run vs the row count in its manifest")
def focus_load_check():
    loaded = (latest_run(spark.read.table("focus_raw"))
              .groupBy("billing_period", "run_id")
              .agg(F.count("*").alias("loaded_rows")))

    # Left join keeps the run even if its manifest is missing (manifest_rows is then NULL and the rule fails)
    return loaded.join(focus_manifests(), "run_id", "left")

# COMMAND ----------

# DBTITLE 1,Copilot usage silver view
# Copilot data quality rules
COPILOT_WARN_RULES = {
    "gross_parsed":          "gross_amount IS NOT NULL",
    "quantity_parsed":       "quantity IS NOT NULL",
    "gross_matches_credits": "abs(gross_amount - quantity * applied_cost_per_quantity) <= 0.000001",  # GitHub's own math checks out
}

# Materialized view: a later export of the same days must REPLACE rows, so not a streaming table
@dp.expect_all(COPILOT_WARN_RULES)                                          # warn: keep the row, count it
@dp.expect_or_drop("has_usage_date", "usage_date IS NOT NULL")              # drop: usage with no date can't go in a daily total
@dp.expect_or_fail("credits_unit", "unit_type = 'ai-credits'")              # fail: totals assume one unit, so stop
@dp.materialized_view(name="finops.silver.copilot_usage",
                      comment="GitHub Copilot AI usage: typed, blanks as NULL, newest export wins per day/user/model/repository. Contains usernames.")
def copilot_usage():
    # Bronze is all strings; try_cast gives NULL on a bad value, nullif turns blank text into NULL
    typed = (spark.read.table("copilot_raw")
             .withColumnRenamed("date", "usage_date")
             .withColumns({
                 "usage_date":                F.expr("try_cast(usage_date AS DATE)"),
                 "quantity":                  F.expr("try_cast(quantity AS DECIMAL(20,10))"),
                 "applied_cost_per_quantity": F.expr("try_cast(applied_cost_per_quantity AS DECIMAL(20,10))"),
                 "gross_amount":              F.expr("try_cast(gross_amount AS DECIMAL(20,10))"),
                 "discount_amount":           F.expr("try_cast(discount_amount AS DECIMAL(20,10))"),
                 "net_amount":                F.expr("try_cast(net_amount AS DECIMAL(20,10))"),
                 "aic_gross_amount":          F.expr("try_cast(aic_gross_amount AS DECIMAL(20,10))"),
                 "aic_quantity":              F.expr("try_cast(aic_quantity AS DECIMAL(20,10))"),
                 "total_monthly_quota":       F.expr("try_cast(total_monthly_quota AS BIGINT)"),
                 "input":                     F.expr("try_cast(input AS BIGINT)"),
                 "output":                    F.expr("try_cast(output AS BIGINT)"),
                 "cache_read":                F.expr("try_cast(cache_read AS BIGINT)"),
                 "cache_write":               F.expr("try_cast(cache_write AS BIGINT)"),
                 "repository":                F.expr("nullif(trim(repository), '')"),
                 "app_client_id":             F.expr("nullif(trim(app_client_id), '')"),
                 "cost_center_name":          F.expr("nullif(trim(cost_center_name), '')"),
             })
             .withColumn("usage_month", F.trunc("usage_date", "month")))

    # One row per day, user, model and repository: keep the one from the newest file
    newest_first = (Window.partitionBy("usage_date", "username", "model", "repository", "product", "sku")
                          .orderBy(F.desc("source_modified_at"), F.desc("source_file")))   # file name breaks ties
    return (typed.withColumn("rn", F.row_number().over(newest_first))
                 .filter("rn = 1")
                 .drop("rn", "_rescued_data"))