# Databricks notebook source
# MAGIC %md
# MAGIC # MAPI Config Copilot
# MAGIC ## Agent Bricks + Unity AI Gateway + MLflow (about 45 min)
# MAGIC
# MAGIC **The story.** Meta text-ad configuration ("MAPI") lives in a shared Google Sheet. On 5 November an approved Real Funeral row
# MAGIC was re-branded to My Comparison Life during testing. The approval stayed in place, and nothing stopped it reaching Meta.
# MAGIC The design: move MAPI to a **Databricks App** with an **AI assistant** that proposes bulk edits, governed by Unity Catalog and Unity AI Gateway.
# MAGIC
# MAGIC ```
# MAGIC   MEX editors / DP approvers
# MAGIC            |
# MAGIC     MAPI UI app (Databricks App, one per person)
# MAGIC            |--Assistant tab: HTTP as the MAPI app's service principal (CAN_USE)-->  Agent Bricks agent app (DurableAgentServer, one per person)
# MAGIC            |                                                                          |--tools-->  search_rows / propose_change_set / find_issues / lookup_rules (UC functions)
# MAGIC            |                                                                          |           (proposes only: no approve, publish or upload tool)
# MAGIC            |                                                                          |--every LLM call-->  Unity AI Gateway model service (routing, rate limits, guardrail, inference table)
# MAGIC            |--approver summaries-->  the same Unity AI Gateway model service
# MAGIC            |
# MAGIC   App state: PGlite (in-memory Postgres inside the app), seeded from Unity Catalog tables (resets when the app restarts)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC ### Run order and timing
# MAGIC Every slow resource is started in `01`, so it is ready before anyone needs it.
# MAGIC
# MAGIC **Agent Bricks, Unity AI Gateway, MLflow**
# MAGIC
# MAGIC | Time | Notebook | What you do | Warming in the background |
# MAGIC |---|---|---|---|
# MAGIC | 0:00 | (presenter) | Story + architecture | - |
# MAGIC | 0:05 | `01_Setup_and_Kickoff` | Run all, then move on | model service, agent app + MAPI app compute (~3 min) |
# MAGIC | 0:08 | `02_Agent_Tools` | Try the assistant's tools by hand; last cell deploys both apps | app deployments (~3-5 min) |
# MAGIC | 0:15 | `03_Unity_AI_Gateway` | Rate limits, guardrail (UI), hot swap; the agent's calls go through the same service | apps still deploying |
# MAGIC | 0:25 | `04_App_and_Assistant` | Use the Agent Bricks agent from your MAPI app; accept a change set; MLflow traces | - |
# MAGIC | 0:35 | `05_Evaluate_with_MLflow` | Test set in UC + LLM judges | - |
# MAGIC
# MAGIC ---
# MAGIC ### Rules for a shared workspace
# MAGIC * **Clone this folder into your own home folder first** (right-click the folder, **Clone**). The `app` and `agent` subfolders must come with it.
# MAGIC * Attach every notebook to **Serverless** compute.
# MAGIC * Everything you create is named `dbx_showcase_<your_user>_mapi*` or `dbxs-mapi-<your_user>-*`. You can only see and delete your own.
# MAGIC * **Run `99_Cleanup` only once you are finished with the app.** It deletes your MAPI app, agent app, model service and everything else you created.
# MAGIC
# MAGIC ### If you get stuck
# MAGIC Every cell can be re-run safely; setup cells reuse anything that already exists. If something is still warming up, move on and come back.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Presenter pre-flight (run once, the day before)
# MAGIC Checks the workspace has what 30 people will need. For a participant-level permissions check, also send `00_Permissions_Preflight`
# MAGIC from the Aurora lab folder - it probes the same APIs this lab uses.

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
check("Summary model in system.ai", lambda: w.registered_models.get(SUMMARY_MODEL).full_name)
check("Fallback model in system.ai", lambda: w.registered_models.get(FALLBACK_MODEL).full_name)
check("Databricks Apps API", lambda: f"{len(list_items('/api/2.0/apps', 'apps'))} apps today")
check("Unity AI Gateway model services API", lambda: f"{len(list_items('/api/2.1/unity-catalog/model-services', 'model_services', {'parent': 'schemas/system.ai'}))} system.ai services")
check("SQL warehouse for the apps", lambda: pick_warehouse_id())
check("Template app code present (./app)", lambda: str(sorted(__import__('os').listdir(f"{notebook_dir()}/app"))))
check("Agent Bricks agent code present (./agent)", lambda: str(sorted(__import__('os').listdir(f"{notebook_dir()}/agent"))))


def _probe_create_schema():
    probe = f"{LAB_CATALOG}._lab_preflight_probe"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {probe} COMMENT 'temporary probe, safe to drop'")
    spark.sql(f"DROP SCHEMA IF EXISTS {probe} CASCADE")
    return "CREATE SCHEMA ok"


check(f"Can create schemas in {LAB_CATALOG}", _probe_create_schema)

for label, status, detail in checks:
    print(f"{status:5} {label:42} {detail}")
failed = [label for label, status, _ in checks if status == "FAIL"]
print("\nPre-flight failed on: " + ", ".join(failed) if failed else "\nAll pre-flight checks passed.")

print(
    "\nCapacity per participant: 2 Databricks Apps (the Agent Bricks agent app and the MAPI UI app) and 1 model service."
    "\nWith 30 people: ~60 apps. Confirm workspace quotas, or run the lab in pairs."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Presenter notes: privileges participants need
# MAGIC
# MAGIC ```sql
# MAGIC -- Unity Catalog
# MAGIC GRANT USE CATALOG, CREATE SCHEMA ON CATALOG sierra_yap_demo_dump_catalog TO `account users`;  -- or a workshop group
# MAGIC ```
# MAGIC
# MAGIC Workspace level (admin console):
# MAGIC * Participants can **create Databricks Apps**.
# MAGIC * Serverless notebooks and a serverless SQL warehouse they can use (the app's service principal is granted `CAN_USE` on it).
# MAGIC * Participants can set permissions on apps they create (`02` grants the MAPI app's service principal `CAN_USE` on the agent app).
# MAGIC * Each app's service principal must be able to `USE CATALOG` on the lab catalog. If the `GRANT USE CATALOG` in `02` prints a note,
# MAGIC   make sure `account users` (which includes service principals) has `USE CATALOG`.
