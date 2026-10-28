# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Pipeline: silver FOCUS costs
# MAGIC Second source file for `finops_cost_pipeline`. Reads `focus_raw` (bronze), fixes types, keeps only the latest export run per billing period.
# MAGIC
# MAGIC **Step 6c:** expectations check every silver row: warn (keep + count), drop (remove), fail (stop the update).
# MAGIC A second view, `cost_reconciliation`, checks that the FOCUS total matches the actual-cost total for each month.
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

# Expectations check one row at a time, so to check a TOTAL we first build one row per month, then put rules on that
@dp.expect("both_sources_present", "focus_billed IS NOT NULL AND actual_billed IS NOT NULL")   # warn: one export missing a month
@dp.expect_or_fail("totals_match",
                   "focus_billed IS NULL OR actual_billed IS NULL OR abs(focus_billed - actual_billed) <= 0.01")  # fail: totals disagree by more than a cent
@dp.materialized_view(name="finops.silver.cost_reconciliation",
                      comment="Billed total per billing period: FOCUS export vs actual-cost export")
def cost_reconciliation():
    # FOCUS side: silver is already typed and deduplicated
    focus = (spark.read.table("finops.silver.focus_costs")
             .groupBy("billing_period")
             .agg(F.sum("BilledCost").alias("focus_billed"),
                  F.count("*").alias("focus_rows")))

    # Actual-cost side: same dedupe, then convert the text column before adding it up
    actual = (latest_run(spark.read.table("actual_raw"))
              .groupBy("billing_period")
              .agg(F.sum(F.expr("try_cast(costInBillingCurrency AS DECIMAL(18,6))")).alias("actual_billed"),
                   F.count("*").alias("actual_rows")))

    # Full outer join keeps a month even if only one export has it
    return (focus.join(actual, "billing_period", "full_outer")
                 .withColumn("difference", F.col("focus_billed") - F.col("actual_billed")))
