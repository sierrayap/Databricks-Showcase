# Databricks notebook source
# MAGIC %md
# MAGIC # Greenstone Next Best Action
# MAGIC ## Genie Code for ML / AI Runtime (30 min)
# MAGIC
# MAGIC **The story.** Greenstone already scores *which* leads are good. What is still open on the roadmap is
# MAGIC **what to do next**: call this morning, call this afternoon, SMS, or email?
# MAGIC
# MAGIC In this lab you build a **Next Best Action** model with Genie Code. Setup and data are already done.
# MAGIC You paste prompts into Genie Code so it **writes a file under `genie/`** for each step. The parent
# MAGIC notebook only `%run`s that file, so you can see the diff and revert one step without editing `02`.
# MAGIC
# MAGIC ```
# MAGIC   contact_history (past outreach + converted Y/N)
# MAGIC            |
# MAGIC     Genie Code  -->  feature prep + sklearn multi-class model  -->  MLflow + Unity Catalog
# MAGIC            |
# MAGIC   leads_today  -->  score every lead x every action  -->  nba_recommendations
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC ### Run order and timing
# MAGIC Attach every notebook to **Serverless**. No classic clusters, no GPUs, no Model Serving.
# MAGIC
# MAGIC | Time | Notebook | What you do |
# MAGIC |---|---|---|
# MAGIC | 0:00 | (presenter) | Story + why NBA is next after lead scoring |
# MAGIC | 0:03 | `01_Setup_and_Data` | **Run all** (~1 min) |
# MAGIC | 0:05 | `02_Build_NBA_with_Genie_Code` | Paste Genie Code prompts; it writes `genie/<step>/code` |
# MAGIC | 0:28 | (optional) | Peek at `nba_recommendations` |
# MAGIC | after | `99_Cleanup` | Run when finished |
# MAGIC
# MAGIC ---
# MAGIC ### Rules for a shared workspace
# MAGIC * **Clone this folder into your own home folder first** (right-click the folder, **Clone**).
# MAGIC * Attach every notebook to **Serverless**.
# MAGIC * Everything you create is named `dbx_showcase_<your_user>_nba*`. You can only see and delete your own.
# MAGIC * Prefer **pandas + scikit-learn** in Genie Code prompts. Do not ask Genie to create clusters, GPUs, or serving endpoints.
# MAGIC
# MAGIC ### If you get stuck
# MAGIC * Open the matching file under `genie/` to see what Genie Code wrote, then re-run the `%run` cell.
# MAGIC * Ask Genie Code to fix **that file** (paste the traceback). To revert, restore the stub `code` notebook.
# MAGIC * Presenter has `02_Build_NBA_with_Genie_Code_SOLUTION`, which `%run`s `genie_solution/` instead.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Presenter pre-flight (run once, the day before)

# COMMAND ----------

# MAGIC %run ./_lab_config

# COMMAND ----------

checks = []


def check(label, fn):
    try:
        detail = fn()
        checks.append((label, "OK", detail or ""))
    except Exception as e:
        checks.append((label, "FAIL", str(e)[:200]))


check("Catalog exists", lambda: w.catalogs.get(LAB_CATALOG).name)


def _probe_create_schema():
    probe = f"{LAB_CATALOG}._lab_preflight_probe_nba"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {probe} COMMENT 'temporary probe, safe to drop'")
    spark.sql(f"DROP SCHEMA IF EXISTS {probe} CASCADE")
    return "CREATE SCHEMA ok"


check(f"Can create schemas in {LAB_CATALOG}", _probe_create_schema)
check("Serverless notebook context", lambda: "ok" if spark is not None else "missing")

for label, status, detail in checks:
    print(f"{status:5} {label:42} {detail}")
failed = [label for label, status, _ in checks if status == "FAIL"]
print("\nPre-flight failed on: " + ", ".join(failed) if failed else "\nAll pre-flight checks passed.")
print(
    "\nCapacity tip: this lab creates only UC tables + 1 MLflow experiment + 1 registered model per person."
    " No serving endpoints. Safe for a full room on Serverless."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Presenter notes: privileges participants need
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CATALOG, CREATE SCHEMA ON CATALOG <lab_catalog> TO `account users`;
# MAGIC ```
# MAGIC
# MAGIC Workspace level:
# MAGIC * Serverless notebooks enabled
# MAGIC * Permission to create MLflow experiments in `/Users/<email>/...`
# MAGIC * Permission to register models in their lab schema (CREATE MODEL on schema, or grant after schema create)
