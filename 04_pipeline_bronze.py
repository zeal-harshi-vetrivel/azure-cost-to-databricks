# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Pipeline: bronze streaming tables
# MAGIC Source code for a Lakeflow Spark Declarative Pipeline. **Don't run this notebook directly** — attach it to a pipeline and click **Run pipeline**.
# MAGIC
# MAGIC Pipeline settings: default catalog `finops`, default schema `bronze`, serverless.

# COMMAND ----------

# DBTITLE 1,Imports and paths
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Landed files from 01_sync_from_storage
EXPORTS_DIR = "/Volumes/finops/raw/real_exports/exports"
# Hand-exported GitHub Copilot usage CSVs
COPILOT_DIR = "/Volumes/finops/raw/copilot_exports/ai_usage"

# COMMAND ----------

# DBTITLE 1,Auto Loader CSV helpers
# General Auto Loader CSV read (pipeline manages checkpoint and schema location)
def read_csv_files(folder, file_glob):
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", file_glob)
        .load(folder)
        .select("*",
                F.col("_metadata.file_path").alias("source_file"),
                F.col("_metadata.file_modification_time").alias("source_modified_at"),
                F.current_timestamp().alias("ingested_at")))

# Thin wrapper for one Azure export folder (data files only, skips manifest.json)
def read_export(export_name):
    return read_csv_files(f"{EXPORTS_DIR}/{export_name}/", "part_*.csv.gz")

# COMMAND ----------

# Streaming table: FOCUS export rows, all columns as strings
@dp.table(name="focus_raw", comment="FOCUS cost export rows, loaded by Auto Loader")
def focus_raw():
    return read_export("george-test-focus-cost")

# COMMAND ----------

# Streaming table: actual-cost export rows (different column layout, so its own table)
@dp.table(name="actual_raw", comment="Actual cost export rows, loaded by Auto Loader")
def actual_raw():
    return read_export("george-test-actual-cost")

# COMMAND ----------

# DBTITLE 1,Copilot raw streaming table
# Streaming table: Copilot AI usage rows, all columns as strings
@dp.table(name="copilot_raw", comment="GitHub Copilot AI usage CSV rows, exported by hand, loaded by Auto Loader. Contains usernames.")
def copilot_raw():
    return read_csv_files(COPILOT_DIR, "*.csv")