# Databricks notebook source
# MAGIC %md
# MAGIC # Step 99 - Clean up everything you created (~1 min)
# MAGIC
# MAGIC Run this **once when you have finished** the NBA lab (or before you leave).
# MAGIC It deletes only resources named for you: registered model, MLflow experiment, and your schema (tables included).
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
        gone = any(s in msg.lower() for s in ("not found", "does not exist", "not_found", "resource_does_not_exist"))
        print(f"{'Gone     ' if gone else 'FAILED   '}{label}  {'' if gone else msg[:200]}")


# Drop UC registered model versions if present
attempt(
    f"registered model {MODEL_NAME}",
    lambda: spark.sql(f"DROP MODEL IF EXISTS {MODEL_NAME}"),
)

try:
    import mlflow

    mlflow.set_tracking_uri("databricks")
    exp = mlflow.get_experiment_by_name(EXPERIMENT_PATH)
    if exp:
        attempt(
            f"MLflow experiment {EXPERIMENT_PATH}",
            lambda: w.experiments.delete_experiment(exp.experiment_id),
        )
    else:
        print(f"Gone      MLflow experiment {EXPERIMENT_PATH}")
except Exception as e:
    print(f"Note      MLflow cleanup: {str(e)[:200]}")

attempt(
    f"schema {FQ_SCHEMA} (tables + registry)",
    lambda: spark.sql(f"DROP SCHEMA IF EXISTS {FQ_SCHEMA} CASCADE"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

left = []
if spark.catalog.databaseExists(FQ_SCHEMA):
    left.append(f"schema {FQ_SCHEMA}")

try:
    import mlflow

    mlflow.set_tracking_uri("databricks")
    if mlflow.get_experiment_by_name(EXPERIMENT_PATH):
        left.append(f"MLflow experiment {EXPERIMENT_PATH}")
except Exception:
    pass

if left:
    print("Still present (re-run this notebook if needed):")
    for item in left:
        print(f"  - {item}")
else:
    print("All clean. Nothing of yours from the Greenstone NBA lab remains. Thanks for joining!")
