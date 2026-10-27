# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 · Sync: copy cost export files from Azure Storage into a volume
# MAGIC Lists the export files in George's storage account, plans what to copy (new / changed / identical),
# MAGIC then copies only new or changed files into `/Volumes/finops/raw/real_exports`, keeping the same folder layout.
# MAGIC
# MAGIC **Before running:** `00_setup` has run, and the SAS token is in secret scope `azure-cost`, key `skunkworks-sas`.
# MAGIC **Safe to re-run:** identical files are skipped.

# COMMAND ----------

# MAGIC %pip install azure-storage-blob

# COMMAND ----------

# Restart Python so the newly installed package can be imported
dbutils.library.restartPython()

# COMMAND ----------

# Configuration - all settings live here and can be changed in one place
ACCOUNT_URL = "https://zbskunkworks.blob.core.windows.net"  # Storage account
CONTAINER   = "cost-management"                             # Container that holds the exports
PREFIX      = "exports/"                                    # Folder inside the container
VOLUME_ROOT = "/Volumes/finops/raw/real_exports"            # Destination: where exports land in Databricks

# Where the SAS token is stored (the name, never the value)
SECRET_SCOPE = "azure-cost"
SECRET_KEY   = "skunkworks-sas"

# COMMAND ----------

# Connect to the storage account
from azure.storage.blob import ContainerClient

# Fetch the SAS token from the secret scope; it is never printed or saved in the notebook
sas = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)

# Client used to list and download files
container_client = ContainerClient(
    account_url=ACCOUNT_URL,   # which storage account
    container_name=CONTAINER,  # which container inside it
    credential=sas,            # the key that grants access
)

# Test the connection by listing every file under the exports folder
for b in container_client.list_blobs(name_starts_with=PREFIX):
    print(b.size, b.name)

# COMMAND ----------

# Plan before copying: decide what to do with each source file
#   new       -> not in the volume yet       -> copy
#   changed   -> same name, different size   -> copy (overwrite)
#   identical -> same name, same size        -> skip
# Limitation: same name + same size + different content -> skipped

import os
from collections import Counter


def decide(blob):
    """Return 'new', 'changed' or 'identical' for one source file."""
    # Mirror the source layout inside the volume:
    # exports/x/y.csv.gz lands at /Volumes/.../exports/x/y.csv.gz
    local_path = f"{VOLUME_ROOT}/{blob.name}"

    # Volumes behave like normal folders, so os.path works on /Volumes/... paths
    if not os.path.exists(local_path):
        return "new"
    if os.path.getsize(local_path) != blob.size:
        return "changed"
    return "identical"


# One decision per source file; the copy cell reads this list
plan = [{"action": decide(b), "name": b.name, "size": b.size}
        for b in container_client.list_blobs(name_starts_with=PREFIX)]

for row in plan:
    print(row["action"].ljust(10), row["size"], row["name"])

print("\nSummary:", dict(Counter(row["action"] for row in plan)))

# COMMAND ----------

# Copy new and changed files into the volume
# (Unity Catalog can't use a SAS token as a credential, so we download with the Python SDK instead)

copied, skipped = 0, 0

for row in plan:
    # Only copy new or changed files
    if row["action"] not in {"new", "changed"}:
        skipped += 1
        continue

    # Same folder layout as the source, inside the volume
    local_path = f"{VOLUME_ROOT}/{row['name']}"

    # Create the destination folders if they're missing
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # Download the file and write it as binary (the CSVs are gzipped)
    with open(local_path, "wb") as f:
        container_client.download_blob(row["name"]).readinto(f)

    # Make sure the copy is the same size as the source
    if os.path.getsize(local_path) != row["size"]:
        raise RuntimeError(f"Size mismatch for {row['name']}")

    print("copied", row["size"], row["name"])
    copied += 1

print(f"Copied {copied}, skipped {skipped}")