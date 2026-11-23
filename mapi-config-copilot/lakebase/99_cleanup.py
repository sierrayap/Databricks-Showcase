# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Part 2 - Clean up the Lakebase lab (~3 min)
# MAGIC
# MAGIC Run this **once, when you have finished part 2**. It removes only what part 2 created, in this order:
# MAGIC
# MAGIC 1. Switches your MAPI app back to PGlite (`USE_LAKEBASE=false`) and removes its Lakebase resource, so it keeps working.
# MAGIC 2. Deletes the synced table (and its pipeline).
# MAGIC 3. Deletes the Unity Catalog catalog you registered for your Lakebase database in step 3, if you did.
# MAGIC 4. Deletes and **purges** your Lakebase project, with every branch, compute and row in it. Purged projects cannot be recovered,
# MAGIC    and the project ID is free again straight away, so you can run part 2 again with the same name.
# MAGIC    With `DELETE_LAKEBASE_PROJECT = False` the project and the app's tables stay; only the synced metrics schema is dropped from Postgres.
# MAGIC 5. Drops your Delta metrics schema `<catalog>.mapi_lakebase_<user>`.
# MAGIC
# MAGIC Your part 1 resources (apps, schema, model service) stay. Remove them afterwards with `../99_Cleanup`.
# MAGIC
# MAGIC **Action:** set your inputs, then **Run all**.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2"
# MAGIC %restart_python

# COMMAND ----------

# ---------------------------------------------------------------------------------------------
# YOUR INPUTS
# ---------------------------------------------------------------------------------------------
PROJECT_NAME = ""  # Your Lakebase project ID or name. "" = mapi-project-<your username>
CATALOG = "databricks_showcase_2026"  # Catalog part 1 used (holds your metrics schema)
LAKEBASE_CATALOG = ""  # The catalog you registered in step 3. "" = mapi_postgres_<your username>
DELETE_LAKEBASE_PROJECT = True  # False keeps the project and its data; everything else is still removed

# COMMAND ----------

# MAGIC %run ./_lakebase_config

# COMMAND ----------


def attempt(label, fn):
    try:
        fn()
        print(f"Done     {label}")
    except Exception as e:
        msg = str(e)
        gone = isinstance(e, NotFound) or any(s in msg.lower() for s in ("not found", "does not exist", "not_found"))
        print(f"{'Gone     ' if gone else 'FAILED   '}{label}  {'' if gone else msg[:200]}")


def app_back_to_pglite():
    app = get_app()
    if not app:
        raise NotFound(f"app {APP_NAME} not found")
    set_app_resources([r for r in (app.get("resources") or []) if not r.get("postgres")])
    wait_for_app_compute(APP_NAME)
    env = app_env("pglite")
    deploy_app(env, env_vars={**env, "USE_LAKEBASE": "false"})


def delete_lakebase_catalog():
    try:
        w.postgres.delete_catalog(name=f"catalogs/{LAKEBASE_CATALOG}").wait()
    except NotFound:
        w.catalogs.delete(LAKEBASE_CATALOG, force=True)  # registered by hand from Catalog Explorer


project = find_project(PROJECT_NAME)

attempt(f"MAPI app {APP_NAME} back on PGlite (redeploying, ~3 min)", app_back_to_pglite)
attempt(f"synced table {SYNCED_TABLE}", lambda: w.postgres.delete_synced_table(name=f"synced_tables/{SYNCED_TABLE}").wait())
attempt(f"catalog {LAKEBASE_CATALOG}", delete_lakebase_catalog)
if DELETE_LAKEBASE_PROJECT:
    if project:
        attempt(f"Lakebase project {project.name} (all branches and data)", lambda: w.postgres.delete_project(name=project.name, purge=True).wait())
    else:
        print(f"Gone     Lakebase project {PROJECT_NAME}")
elif project:
    def drop_synced_pg_schema():
        with pg_connect(lakebase_target(PROJECT_NAME, "production")) as conn:
            conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(METRICS_SCHEMA)))

    attempt(f"Postgres schema {METRICS_SCHEMA} (synced metrics copy)", drop_synced_pg_schema)
    print(f"Kept     Lakebase project {project.name} (DELETE_LAKEBASE_PROJECT = False)")
attempt(f"schema {METRICS_FQ_SCHEMA} (metrics table, sync pipeline storage)", lambda: spark.sql(f"DROP SCHEMA IF EXISTS {METRICS_FQ_SCHEMA} CASCADE"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

left = []
if spark.catalog.databaseExists(METRICS_FQ_SCHEMA):
    left.append(f"schema {METRICS_FQ_SCHEMA}")
if spark.sql(f"SHOW CATALOGS LIKE '{LAKEBASE_CATALOG}'").count():
    left.append(f"catalog {LAKEBASE_CATALOG}")
if DELETE_LAKEBASE_PROJECT and find_project(PROJECT_NAME):
    left.append(f"Lakebase project {PROJECT_NAME}")
app = get_app()
if app and any(r.get("postgres") for r in app.get("resources") or []):
    left.append(f"Lakebase resource on app {APP_NAME}")

if left:
    print("Still present (re-run the cleanup cell, or check the messages above):")
    for item in left:
        print(f"  - {item}")
else:
    print("All clean. Nothing from part 2 remains." + (" Your MAPI app is redeploying on PGlite." if app else ""))
    print("Run ../99_Cleanup to remove the part 1 resources too.")
