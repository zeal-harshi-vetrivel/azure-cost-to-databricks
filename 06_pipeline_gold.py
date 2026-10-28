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

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

# The columns every cost source must provide (FOCUS names), so gold never depends on one provider
FOCUS_COLUMNS = [
    "ProviderName", "SubAccountName",
    "ServiceCategory", "ServiceName",
    "ResourceId", "ResourceName",
    "ChargePeriodStart", "BillingPeriodStart",
    "BilledCost", "EffectiveCost", "ListCost",
    "ConsumedQuantity", "ConsumedUnit",
]

# Temporary view: a named query inside the pipeline only, not saved as a table, nothing stored
@dp.temporary_view(name="all_costs", comment="Every cost source in FOCUS columns; the one place new sources are added")
def all_costs():
    azure = (spark.read.table("finops.silver.focus_costs")
             .select(*FOCUS_COLUMNS)
             .withColumn("x_Source", F.lit("azure_cost_export")))   # which feed a row came from
    # Step 7: return azure.unionByName(copilot, allowMissingColumns=True)
    return azure

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
