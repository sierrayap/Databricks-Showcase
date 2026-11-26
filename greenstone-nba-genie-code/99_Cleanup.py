# Databricks notebook source
# MAGIC %md
# MAGIC # Step 99 - Clean up everything you created (~1 min)
# MAGIC
# MAGIC Run this **once when you have finished** the NBA lab (or before you leave).
# MAGIC It deletes only resources named for you: registered model, MLflow experiment, and your schema (tables included).
# MAGIC It also resets the four Genie Code notebooks under `genie/` back to empty stubs, so any code Genie wrote is cleared.
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
# MAGIC ### Reset Genie Code notebooks
# MAGIC Clears the four notebooks Genie Code writes into (`02_1_explore` … `02_4_score`), so the next run starts from empty stubs.
# MAGIC Leaves `genie_solution/` alone.

# COMMAND ----------

from databricks.sdk.service.workspace import ImportFormat, Language

_GENIE_CODE_STUBS = (
    ("genie/02_1_explore/code", "02_1_explore — Genie Code writes this file"),
    ("genie/02_2_features/code", "02_2_features — Genie Code writes this file"),
    ("genie/02_3_train/code", "02_3_train — Genie Code writes this file"),
    ("genie/02_4_score/code", "02_4_score — Genie Code writes this file"),
)


def _lab_folder():
    path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    return path.rsplit("/", 1)[0]


def _stub_source(rel_path, title):
    run_path = "./" + rel_path
    return (
        "# Databricks notebook source\n"
        "# MAGIC %md\n"
        f"# MAGIC # {title}\n"
        "# MAGIC\n"
        "# MAGIC Do not paste code into `02_Build_NBA_with_Genie_Code`. Ask Genie Code to implement **this** notebook, "
        f"then re-run `%run {run_path}` in `02`.\n"
    )


def reset_genie_code(rel_path, title):
    content = _stub_source(rel_path, title).encode("utf-8")
    w.workspace.upload(
        f"{_lab_folder()}/{rel_path}",
        content,
        overwrite=True,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
    )


for rel_path, title in _GENIE_CODE_STUBS:
    attempt(f"Genie code stub {rel_path}", lambda p=rel_path, t=title: reset_genie_code(p, t))

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

for rel_path, _ in _GENIE_CODE_STUBS:
    try:
        exported = w.workspace.download(f"{_lab_folder()}/{rel_path}").read().decode("utf-8")
        # Anything beyond the stub header means Genie (or someone) still has code in the notebook.
        if "# COMMAND ----------" in exported or "DBTITLE" in exported:
            left.append(f"Genie code still has body: {rel_path}")
    except Exception as e:
        left.append(f"Genie code missing or unreadable: {rel_path} ({str(e)[:80]})")

if left:
    print("Still present (re-run this notebook if needed):")
    for item in left:
        print(f"  - {item}")
else:
    print("All clean. Nothing of yours from the Greenstone NBA lab remains. Thanks for joining!")
