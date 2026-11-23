# Databricks notebook source
# MAGIC %md
# MAGIC # Lab config (shared by every notebook in this lab)
# MAGIC
# MAGIC **You do not need to edit or run this notebook directly** - every lab notebook pulls it in with `%run ./_lab_config`.
# MAGIC
# MAGIC It works out **your** resource names from your login (so many people never collide) and keeps a small registry
# MAGIC so `99_Cleanup` deletes exactly what you created.
# MAGIC
# MAGIC | Resource | Name |
# MAGIC |---|---|
# MAGIC | Schema | `<catalog>.dbx_showcase_<user>_nba` |
# MAGIC | Tables | `leads_today`, `contact_history`, `nba_recommendations` |
# MAGIC | Model | `<schema>.nba_next_best_action` |
# MAGIC | MLflow experiment | `/Users/<you>/dbx_showcase_<user>_nba` |

# COMMAND ----------

# Presenter: set these once before the workshop. Participants never change them.
LAB_CATALOG = "sierra_yap_demo_dump_catalog"  # Change to the workshop catalog in Greenstone workspaces
LAB_CODE = "nba"

# Keep the lab fast: small data, CPU only, no serving endpoints, no GPUs.
N_HISTORY_ROWS = 8000
N_LEADS_TODAY = 400
RANDOM_SEED = 42
ACTIONS = ["call_am", "call_pm", "sms", "email"]

# COMMAND ----------

import hashlib
import re

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
USER_EMAIL = w.current_user.me().user_name
USER_SLUG = re.sub(r"[^a-z0-9]+", "_", USER_EMAIL.split("@")[0].lower()).strip("_")

PREFIX = f"dbx_showcase_{USER_SLUG}_{LAB_CODE}"
SCHEMA = PREFIX
FQ_SCHEMA = f"{LAB_CATALOG}.{SCHEMA}"

LEADS_TABLE = f"{FQ_SCHEMA}.leads_today"
HISTORY_TABLE = f"{FQ_SCHEMA}.contact_history"
RECS_TABLE = f"{FQ_SCHEMA}.nba_recommendations"
MODEL_NAME = f"{FQ_SCHEMA}.nba_next_best_action"
EXPERIMENT_PATH = f"/Users/{USER_EMAIL}/{PREFIX}"
REGISTRY_TABLE = f"{FQ_SCHEMA}.lab_resources"

# COMMAND ----------


def register(resource_type, resource_id, display_name=""):
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} "
        "(resource_type STRING, resource_id STRING, display_name STRING, created_at TIMESTAMP)"
    )
    spark.sql(
        f"DELETE FROM {REGISTRY_TABLE} WHERE resource_type = :t AND resource_id = :i",
        args={"t": resource_type, "i": resource_id},
    )
    spark.sql(
        f"INSERT INTO {REGISTRY_TABLE} VALUES (:t, :i, :n, current_timestamp())",
        args={"t": resource_type, "i": resource_id, "n": display_name},
    )


def registered(resource_type):
    if not spark.catalog.tableExists(REGISTRY_TABLE):
        return []
    rows = spark.sql(
        f"SELECT resource_id FROM {REGISTRY_TABLE} WHERE resource_type = :t",
        args={"t": resource_type},
    ).collect()
    return [r.resource_id for r in rows]


print(f"Hello {USER_EMAIL}")
print(f"  Your lab prefix : {PREFIX}")
print(f"  Your schema     : {FQ_SCHEMA}")
print(f"  History table   : {HISTORY_TABLE}")
print(f"  Leads table     : {LEADS_TABLE}")
print(f"  Model name      : {MODEL_NAME}")
