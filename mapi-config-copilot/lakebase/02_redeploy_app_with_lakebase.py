# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Part 2 - Step 4: point the app at Lakebase with a feature flag (~5 min, mostly waiting)
# MAGIC
# MAGIC The app code from part 1 already speaks Postgres. One environment variable decides where that Postgres is:
# MAGIC
# MAGIC | `USE_LAKEBASE` | Storage | What happens to edits on restart |
# MAGIC |---|---|---|
# MAGIC | unset / `false` (part 1) | PGlite, in memory inside the app | Lost |
# MAGIC | `true` (this notebook) | Your Lakebase project (`LAKEBASE_PROJECT`, `LAKEBASE_BRANCH`) | Kept |
# MAGIC
# MAGIC This notebook gives the app's service principal access to your Lakebase database, then **redeploys the same code through the
# MAGIC Apps API with the flag flipped**. Nothing in the source or `app.yaml` changes.
# MAGIC
# MAGIC **Action:** set your inputs, then **Run all**. While the app redeploys (~3 min), the presenter covers autoscaling and branching.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2"
# MAGIC %restart_python

# COMMAND ----------

# ---------------------------------------------------------------------------------------------
# YOUR INPUTS
# ---------------------------------------------------------------------------------------------
PROJECT_NAME = ""  # Your Lakebase project ID or name. "" = mapi-project-<your username>
BRANCH = "production"  # Branch the app should use
CATALOG = "databricks_showcase_2026"  # Catalog part 1 used

# COMMAND ----------

# MAGIC %run ./_lakebase_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Check the starting point
# MAGIC Your MAPI app from part 1 must exist, and step 1 must have seeded Lakebase.

# COMMAND ----------

target = lakebase_target(PROJECT_NAME, BRANCH)
app = get_app()
if not app:
    raise RuntimeError(f"App {APP_NAME} not found. Run part 1 (../01_Setup_and_Kickoff and ../02_Agent_Tools) first, with the same CATALOG.")
with pg_connect(target) as conn:
    found = conn.execute("SELECT to_regclass(%s)", (f"{PG_SCHEMA}.mapi_rows",)).fetchone()[0]
    n_rows = conn.execute(f"SELECT count(*) FROM {PG_SCHEMA}.mapi_rows").fetchone()[0] if found else 0
if not n_rows:
    raise RuntimeError(f"{PG_SCHEMA}.mapi_rows is empty or missing on {target['branch']}. Run 01_seed_lakebase_project first.")
has_lakebase = any(r.get("postgres") for r in app.get("resources") or [])
print(f"App {APP_NAME}: {(app.get('app_status') or {}).get('state')}, Lakebase resource attached: {'yes' if has_lakebase else 'no (PGlite)'}")
print(f"Lakebase {target['branch']}: {n_rows} rows in {PG_SCHEMA}.mapi_rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Give the app access to your Lakebase database
# MAGIC 1. Add a **Lakebase resource** to the app (branch + database, `CAN_CONNECT_AND_CREATE`). This creates a Postgres login for the
# MAGIC    app's service principal. The warehouse and model service resources from part 1 are kept.
# MAGIC 2. Give the service principal `CAN_USE` on the project, so the app can look up the project's compute by name.
# MAGIC 3. The tables were created by **you** in step 1, so you grant the app's login what it needs on schema `mapi`: read and write rows,
# MAGIC    nothing else (no `DROP`, no `ALTER`).
# MAGIC
# MAGIC The app never stores a password: it asks for a fresh OAuth token for its own identity when it opens a connection.

# COMMAND ----------

lakebase_resource = {
    "name": "lakebase",
    "postgres": {"branch": target["branch"], "database": target["database"], "permission": "CAN_CONNECT_AND_CREATE"},
}
resources = [r for r in (app.get("resources") or []) if not r.get("postgres")] + [lakebase_resource]
set_app_resources(resources)

sp = wait_for_app_sp()
api("PATCH", f"/api/2.0/permissions/database-projects/{target['project_id']}",
    body={"access_control_list": [{"service_principal_name": sp, "permission_level": "CAN_USE"}]})
ensure_app_role(target, sp)
with pg_connect(target) as conn:
    grant_schema(conn, PG_SCHEMA, sp)
    grants = pg_query(conn, "SELECT table_name, string_agg(privilege_type, ', ' ORDER BY privilege_type) AS privileges "
                      "FROM information_schema.role_table_grants WHERE grantee = %s AND table_schema = %s GROUP BY 1 ORDER BY 1", (sp, PG_SCHEMA))
print(f"App service principal {sp} on {target['branch']}:")
display(grants)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Redeploy with the flag flipped, through the Apps API
# MAGIC The deployment request carries the environment. It is the part 1 environment plus the flag and the Lakebase settings:
# MAGIC
# MAGIC ```
# MAGIC POST /api/2.0/apps/<your app>/deployments
# MAGIC {
# MAGIC   "source_code_path": "<the same source folder as part 1>",
# MAGIC   "mode": "SNAPSHOT",
# MAGIC   "env_vars": [ ...part 1 settings...,
# MAGIC                 {"name": "USE_LAKEBASE",     "value": "true"},
# MAGIC                 {"name": "LAKEBASE_PROJECT", "value": "<your project>"},
# MAGIC                 {"name": "LAKEBASE_BRANCH",  "value": "production"}, ... ]
# MAGIC }
# MAGIC ```
# MAGIC `env_vars` on a deployment replace the `env` list in `app.yaml` for that deployment (the `command` still comes from `app.yaml`),
# MAGIC so the request carries the complete environment, not just the flag. The Apps API never returns the values when you read a deployment back.
# MAGIC
# MAGIC With the flag on, the app looks up the read-write endpoint of `LAKEBASE_PROJECT` / `LAKEBASE_BRANCH` and opens a small
# MAGIC connection pool as its service principal. The SQL it runs is the same SQL it ran against PGlite.
# MAGIC To switch back, redeploy with `USE_LAKEBASE=false` (`99_cleanup` does that).

# COMMAND ----------

env = app_env("pglite")  # exactly the settings part 1 deployed with (../02_Agent_Tools, step 2.4)
flags = lakebase_app_env(target)
print("Flag and Lakebase settings sent with the deployment:")
print(json.dumps(flags, indent=2))
deployment = deploy_app(env, env_vars={**env, **flags})
show_link("MAPI app (manage)", app_manage_url())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Steps 5 and 6 - while the app redeploys (presenter, in the Lakebase UI)
# MAGIC Nothing to run here. Follow along in **Lakebase** (app switcher): the compute's autoscaling range and scale-to-zero,
# MAGIC then branching. Come back and run the next cell when the presenter says so.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Wait for the deployment, then open the app

# COMMAND ----------

app = wait_for_deployment(APP_NAME)
while (app.get("active_deployment") or {}).get("deployment_id") != deployment.get("deployment_id"):
    time.sleep(15)  # the new deployment has not been picked up yet
    app = wait_for_deployment(APP_NAME)
print(f"Deployment {deployment.get('deployment_id')} is live: USE_LAKEBASE=true, LAKEBASE_PROJECT={flags['LAKEBASE_PROJECT']}, "
      f"LAKEBASE_BRANCH={flags['LAKEBASE_BRANCH']}")
show_link("MAPI app (open)", app.get("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Hands on: edits that survive a restart
# MAGIC 1. Open the app. The caption under the title now says **storage: Lakebase Postgres**, and the Editor shows ~400 rows (part 1 had 126).
# MAGIC 2. Edit a row (for example the landing URL of an approved row) and save it. The row goes back to draft: that is the database trigger.
# MAGIC 3. **Row history** for `R-0068` now shows the whole incident: approved on 30 October, re-branded in the sheet on 5 November.
# MAGIC 4. The **Metrics** tab says *Coming soon...*. You fill it in step 7.
# MAGIC 5. Run the next cell: your edit is in Lakebase, and the app's own connections show up in Postgres.
# MAGIC 6. Optional: **Stop** and **Start** the app on its manage page. Your edit is still there. Under PGlite it would have been lost.

# COMMAND ----------

with pg_connect(target) as conn:
    print("Latest changes in Lakebase:")
    display(pg_query(conn, f"SELECT changed_at, row_id, field, new_value, changed_by, source FROM {PG_SCHEMA}.change_log ORDER BY changed_at DESC LIMIT 5"))
    sessions = pg_query(conn, "SELECT usename AS login, count(*) AS connections, max(backend_start) AS newest FROM pg_stat_activity "
                        "WHERE usename = %s GROUP BY 1", (sp,))
print("The app's service principal connected to Postgres:" if not sessions.empty else "No app connections yet: open the app, then re-run this cell.")
if not sessions.empty:
    display(sessions)

# COMMAND ----------

# MAGIC %md
# MAGIC **Next:** `03_metrics_and_sync`.
