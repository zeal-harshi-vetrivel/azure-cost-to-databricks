# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 · Bronze: load cost exports with Auto Loader
# MAGIC Same job as `02_bronze_copy_into`, done with Auto Loader into a separate table so the two can be compared.
# MAGIC
# MAGIC **Safe to re-run:** the checkpoint remembers which files were already loaded.

# COMMAND ----------

from pyspark.sql import functions as F

# Same widgets as 02, but a different target table so the COPY INTO table is untouched
dbutils.widgets.text("catalog", "finops")
dbutils.widgets.text("export_name", "george-test-focus-cost")
dbutils.widgets.text("target_table", "focus_costs_al")

CATALOG      = dbutils.widgets.get("catalog")
EXPORT_NAME  = dbutils.widgets.get("export_name")
TABLE_NAME   = dbutils.widgets.get("target_table")

SOURCE_DIR   = f"/Volumes/{CATALOG}/raw/real_exports/exports/{EXPORT_NAME}/"
TARGET_TABLE = f"{CATALOG}.bronze.{TABLE_NAME}"
CHECKPOINT   = f"/Volumes/{CATALOG}/raw/checkpoints/{TABLE_NAME}"   # one checkpoint per target table

# COMMAND ----------

# A volume to hold Auto Loader's checkpoints and inferred schemas
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.checkpoints COMMENT 'Auto Loader checkpoints and schemas'")

# COMMAND ----------

# Read: Auto Loader ("cloudFiles") finds new files in SOURCE_DIR and its subfolders
stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("pathGlobFilter", "part_*.csv.gz")          # data files only; skips manifest.json
    .option("cloudFiles.schemaLocation", CHECKPOINT)    # where the inferred schema is saved
    .load(SOURCE_DIR)
    .select("*",
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingested_at")))

# Write: process everything new, then stop (serverless only supports this type of trigger)
query = (stream.writeStream
    .option("checkpointLocation", CHECKPOINT)            # where loaded files are remembered
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE))

query.awaitTermination()

# Rows read in this run; 0 on a re-run means nothing new
print("rows read this run:", sum(p["numInputRows"] or 0 for p in query.recentProgress))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if it has the same row count as the COPY INTO table (_rescued_data should be NULL on every row)
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM finops.bronze.focus_costs)    AS copy_into_rows,
# MAGIC   (SELECT count(*) FROM finops.bronze.focus_costs_al) AS auto_loader_rows,
# MAGIC   (SELECT count(*) FROM finops.bronze.focus_costs_al WHERE _rescued_data IS NOT NULL) AS rescued_rows;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Auto Loader added _rescued_data; every source column is STRING because CSV types aren't inferred by default
# MAGIC DESCRIBE TABLE finops.bronze.focus_costs_al;

# COMMAND ----------

# Peek inside the checkpoint: the schema Auto Loader inferred and saved
display(dbutils.fs.ls(f"{CHECKPOINT}/_schemas"))
print(dbutils.fs.head(f"{CHECKPOINT}/_schemas/0", 1500))