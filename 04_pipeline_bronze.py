# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Pipeline: bronze streaming tables
# MAGIC Source code for a Lakeflow Spark Declarative Pipeline. **Don't run this notebook directly** — attach it to a pipeline and click **Run pipeline**.
# MAGIC
# MAGIC Pipeline settings: default catalog `finops`, default schema `bronze`, serverless.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Landed files from 01_sync_from_storage
EXPORTS_DIR = "/Volumes/finops/raw/real_exports/exports"

# COMMAND ----------

# Reusable Auto Loader read for one export folder (no checkpoint or schemaLocation: the pipeline manages both)
def read_export(export_name):
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", "part_*.csv.gz")      # data files only; skips manifest.json
        .load(f"{EXPORTS_DIR}/{export_name}/")
        .select("*",
                F.col("_metadata.file_path").alias("source_file"),
                F.col("_metadata.file_modification_time").alias("source_modified_at"),   # when Azure wrote the file
                F.current_timestamp().alias("ingested_at")))

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
