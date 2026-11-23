# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Step 99 - Clean up everything you created (~2 min)
# MAGIC
# MAGIC Run this **once, when you have finished with the MAPI session** (or before you leave, even if you did not finish).
# MAGIC Do not run it if another session still needs your app or assistant.
# MAGIC It deletes only resources named for you: model service, MAPI app, Agent Bricks agent app,
# MAGIC MLflow experiment, code folders and your schema.
# MAGIC
# MAGIC **Action:** Run all.

# COMMAND ----------

# MAGIC %run ./_lab_config

# COMMAND ----------


def attempt(label, fn):
    try:
        fn()
        print(f"Deleted  {label}")
    except Exception as e:
        msg = str(e)
        gone = any(s in msg.lower() for s in ("not found", "does not exist", "not_found"))
        print(f"{'Gone     ' if gone else 'FAILED   '}{label}  {'' if gone else msg[:200]}")


schema_exists = spark.catalog.databaseExists(FQ_SCHEMA)

attempt(f"model service {MODEL_SERVICE_FQN}", lambda: api("DELETE", f"/api/2.1/unity-catalog/model-services/{MODEL_SERVICE_FQN}"))
attempt(f"MAPI app {APP_NAME}", lambda: api("DELETE", f"/api/2.0/apps/{APP_NAME}"))
attempt(f"agent app {AGENT_APP_NAME}", lambda: api("DELETE", f"/api/2.0/apps/{AGENT_APP_NAME}"))

try:
    import mlflow

    mlflow.set_tracking_uri("databricks")
    exp = mlflow.get_experiment_by_name(EXPERIMENT_PATH)
    if exp:
        attempt(f"MLflow experiment {EXPERIMENT_PATH}", lambda: w.experiments.delete_experiment(exp.experiment_id))
except Exception:
    pass

attempt(f"app code folder {APP_SOURCE_PATH}", lambda: w.workspace.delete(APP_SOURCE_PATH.removeprefix("/Workspace"), recursive=True))
attempt(f"agent code folder {AGENT_SOURCE_PATH}", lambda: w.workspace.delete(AGENT_SOURCE_PATH.removeprefix("/Workspace"), recursive=True))
attempt(f"guardrail function {GUARDRAIL_FN}", lambda: spark.sql(f"DROP FUNCTION IF EXISTS {GUARDRAIL_FN}"))
attempt(f"schema {FQ_SCHEMA} (tables, views, volume, functions, inference table)", lambda: spark.sql(f"DROP SCHEMA IF EXISTS {FQ_SCHEMA} CASCADE"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

time.sleep(10)
left = []
if spark.catalog.databaseExists(FQ_SCHEMA):
    left.append(f"schema {FQ_SCHEMA}")
for name in (APP_NAME, AGENT_APP_NAME):
    app = get_app(name)
    if app and (app.get("compute_status") or {}).get("state") != "DELETING":
        left.append(f"app {name}")


if left:
    print("Still present (endpoint and app teardown can lag a minute - re-run this cell):")
    for item in left:
        print(f"  - {item}")
else:
    print(f"All clean. Nothing of yours from the MAPI lab remains. Thanks for joining!")