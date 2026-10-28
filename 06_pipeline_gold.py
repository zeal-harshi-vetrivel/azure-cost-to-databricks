# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Pipeline: gold cost tables
# MAGIC Third source file for `finops_cost_pipeline`. Builds business-ready tables from silver.
# MAGIC
# MAGIC **Provider-neutral:** every gold table reads one view, `all_costs`, with fixed FOCUS columns.
# MAGIC Step 7 adds GitHub Copilot to that view, and every gold table picks it up with no other changes.
# MAGIC
# MAGIC **Before the first run:** `CREATE SCHEMA IF NOT EXISTS finops.gold;`

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,FOCUS columns and all_costs view
# The columns every cost source must provide (FOCUS names), so gold never depends on one provider
FOCUS_COLUMNS = [
    "ProviderName", "SubAccountName",
    "ServiceCategory", "ServiceName",
    "ResourceId", "ResourceName",
    "ChargePeriodStart", "BillingPeriodStart",
    "BilledCost", "EffectiveCost", "ListCost",
    "ConsumedQuantity", "ConsumedUnit",
]

# One shared number type so every source adds up without rounding (Copilot amounts have 9 decimals)
AMOUNT_TYPE = "DECIMAL(22,10)"

# Temporary view: a named query inside the pipeline only, not saved as a table, nothing stored
@dp.temporary_view(name="all_costs", comment="Every cost source in FOCUS columns; the one place new sources are added")
def all_costs():
    azure = (spark.read.table("finops.silver.focus_costs")
             .select(*FOCUS_COLUMNS)
             .withColumn("x_Source", F.lit("azure_cost_export")))   # which feed a row came from

    # Copilot rows renamed into FOCUS columns; usernames stay out of gold
    copilot = (spark.read.table("finops.silver.copilot_usage")
               .select(
                   F.lit("GitHub").alias("ProviderName"),
                   F.col("organization").alias("SubAccountName"),
                   F.lit("AI and Machine Learning").alias("ServiceCategory"),
                   F.lit("GitHub Copilot").alias("ServiceName"),
                   F.concat(F.lit("github/copilot/"), F.col("model")).alias("ResourceId"),
                   F.col("model").alias("ResourceName"),
                   F.col("usage_date").cast("timestamp").alias("ChargePeriodStart"),
                   F.col("usage_month").cast("timestamp").alias("BillingPeriodStart"),
                   F.col("net_amount").alias("BilledCost"),       # what GitHub invoices: 0 while within quota
                   F.col("net_amount").alias("EffectiveCost"),
                   F.col("gross_amount").alias("ListCost"),       # credits at list price, before the quota discount
                   F.col("quantity").alias("ConsumedQuantity"),
                   F.col("unit_type").alias("ConsumedUnit"))
               .withColumn("x_Source", F.lit("github_copilot_usage")))

    # Stack the sources by column name, then give every amount the same type
    return (azure.unionByName(copilot)
                 .withColumns({c: F.col(c).cast(AMOUNT_TYPE)
                               for c in ["BilledCost", "EffectiveCost", "ListCost", "ConsumedQuantity"]}))

# COMMAND ----------

# Gold 1: cost per day per service (daily trend charts)
@dp.materialized_view(name="finops.gold.daily_cost_by_service",
                      comment="Billed, effective and list cost per day, provider and service")
def daily_cost_by_service():
    return (spark.read.table("all_costs")
            .withColumn("usage_date", F.to_date("ChargePeriodStart"))
            .groupBy("usage_date", "ProviderName", "ServiceCategory", "ServiceName")
            .agg(F.sum("BilledCost").alias("billed_cost"),
                 F.sum("EffectiveCost").alias("effective_cost"),
                 F.sum("ListCost").alias("list_cost"),
                 F.count("*").alias("charge_rows")))

# COMMAND ----------

# Gold 2: cost per resource per month (what costs the most)
@dp.materialized_view(name="finops.gold.monthly_cost_by_resource",
                      comment="Billed and effective cost per billing month and resource, with days the resource was charged")
def monthly_cost_by_resource():
    return (spark.read.table("all_costs")
            .withColumn("billing_month", F.to_date("BillingPeriodStart"))
            .groupBy("billing_month", "ProviderName", "SubAccountName", "ServiceName", "ResourceName")
            .agg(F.sum("BilledCost").alias("billed_cost"),
                 F.sum("EffectiveCost").alias("effective_cost"),
                 F.countDistinct(F.to_date("ChargePeriodStart")).alias("days_charged")))

# COMMAND ----------

# Gold 3: one row per month per provider (headline numbers)
@dp.expect_or_fail("summary_has_cost", "billed_cost IS NOT NULL")   # a NULL total means silver is broken; stop before George sees it
@dp.materialized_view(name="finops.gold.monthly_summary",
                      comment="Monthly totals per provider: cost, discount vs list price, approximate resource and service counts")
def monthly_summary():
    return (spark.read.table("all_costs")
            .withColumn("billing_month", F.to_date("BillingPeriodStart"))
            .groupBy("billing_month", "ProviderName")
            .agg(F.sum("BilledCost").alias("billed_cost"),
                 F.sum("EffectiveCost").alias("effective_cost"),
                 F.sum("ListCost").alias("list_cost"),
                 (F.sum("ListCost") - F.sum("EffectiveCost")).alias("savings_vs_list"),
                 F.approx_count_distinct("ResourceId").alias("approx_resources"),   # fast estimate, ~5% error by default
                 F.countDistinct("ResourceId").alias("exact_resources"),            # exact, for comparison while learning
                 F.approx_count_distinct("ServiceName").alias("approx_services")))

# COMMAND ----------

# DBTITLE 1,Gold 4: Copilot monthly
# Gold 4: Copilot per month. Included credits are POOLED across seats, so report how many seats' worth we used
@dp.expect("credits_match_list_cost", "abs(credits_used * 0.01 - list_cost) <= 0.01")   # warn: 1 AI credit = $0.01
@dp.expect_or_fail("has_active_users", "active_users > 0")                              # fail: a month with cost but no users is broken
@dp.materialized_view(name="finops.gold.copilot_monthly",
                      comment="GitHub Copilot per month: credits used, active users, and how many seats' worth of included credits that equals (credits are pooled across seats)")
def copilot_monthly():
    return (spark.read.table("finops.silver.copilot_usage")
            .groupBy("usage_month")
            .agg(F.sum("quantity").alias("credits_used"),
                 F.sum("gross_amount").alias("list_cost"),
                 F.sum("net_amount").alias("billed_cost"),
                 F.countDistinct("username").alias("active_users"),
                 F.max("total_monthly_quota").alias("included_credits_per_seat"))
            .withColumns({
                "credits_per_active_user": F.round(F.try_divide("credits_used", "active_users"), 2),
                "seats_of_credits_used":   F.round(F.try_divide("credits_used", "included_credits_per_seat"), 2),
            }))

# COMMAND ----------

# DBTITLE 1,Gold 5: Copilot per user monthly
# Gold 5: Copilot per user per month (contains usernames; Step 8 masks them)
@dp.materialized_view(name="finops.gold.copilot_user_monthly",
                      comment="GitHub Copilot per user per month. pct_of_one_seat compares with one seat's included credits; credits are pooled, so it is a fair-share signal, not a limit. Contains usernames.")
def copilot_user_monthly():
    per_user = (spark.read.table("finops.silver.copilot_usage")
                .groupBy("usage_month", "username")
                .agg(F.sum("quantity").alias("credits_used"),
                     F.sum("gross_amount").alias("list_cost"),
                     F.sum("net_amount").alias("billed_cost"),
                     F.countDistinct("usage_date").alias("active_days"),
                     F.countDistinct("model").alias("models_used"),
                     F.sum("output").alias("output_tokens"),
                     F.max("total_monthly_quota").alias("included_credits_per_seat")))

    # Window over the month: each user's share of the org total, without collapsing rows
    month = Window.partitionBy("usage_month")
    return per_user.withColumns({
        "share_of_org_credits": F.round(F.try_divide(F.col("credits_used"), F.sum("credits_used").over(month)), 4),
        "pct_of_one_seat":      F.round(F.try_divide("credits_used", "included_credits_per_seat"), 4),
    })

# COMMAND ----------

# DBTITLE 1,Gold 6: Copilot per model monthly
# Gold 6: Copilot per model per month (model mix; no usernames)
@dp.materialized_view(name="finops.gold.copilot_model_monthly",
                      comment="GitHub Copilot per model per month: credits, list cost, users and share of the month's credits")
def copilot_model_monthly():
    per_model = (spark.read.table("finops.silver.copilot_usage")
                 .groupBy("usage_month", "model")
                 .agg(F.sum("quantity").alias("credits_used"),
                      F.sum("gross_amount").alias("list_cost"),
                      F.countDistinct("username").alias("users"),
                      F.sum("output").alias("output_tokens")))

    # Each model's share of the month's credits, without collapsing rows
    month = Window.partitionBy("usage_month")
    return per_model.withColumn("share_of_credits",
        F.round(F.try_divide(F.col("credits_used"), F.sum("credits_used").over(month)), 4))