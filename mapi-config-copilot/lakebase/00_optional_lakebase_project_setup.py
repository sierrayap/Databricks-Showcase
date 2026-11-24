# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Part 2 - Step 0 (optional): create your Lakebase project
# MAGIC
# MAGIC Part 2 moves the MAPI app's data out of PGlite (in-memory, gone on every restart) into **Lakebase**, Databricks'
# MAGIC serverless Postgres. Run it straight after part 1: your MAPI app, agent app and schema must still exist.
# MAGIC
# MAGIC | Step | Notebook | What happens |
# MAGIC |---|---|---|
# MAGIC | 1 | **create the project by hand** (or this notebook) | A Lakebase project: `production` branch, `databricks_postgres` database, autoscaling compute |
# MAGIC | 2 | `01_seed_lakebase_project` | The app's tables in Postgres, seeded with a year of MAPI history |
# MAGIC | 3 | (Catalog Explorer + SQL editor) | Register the database in Unity Catalog and query it |
# MAGIC | 4 | `02_redeploy_app_with_lakebase` | Redeploy the app with the `USE_LAKEBASE` flag on |
# MAGIC | 5-6 | (Lakebase UI, presenter) | Autoscaling, branching, drop a schema and restore it |
# MAGIC | 7 | `03_metrics_and_sync` | Aggregate in Delta, sync back to Lakebase, see the Metrics tab |
# MAGIC | end | `99_cleanup` | Remove everything part 2 created |
# MAGIC
# MAGIC ### Most of the time you create the project by hand
# MAGIC 1. Open **Lakebase** from the app switcher (top right) and click **New project**.
# MAGIC 2. Name it **`mapi-project-<your username>`**: the part of your email before `@`, with every character that is not a lowercase
# MAGIC    letter, digit or `-` replaced by `-`. For `alex.feng@databricks.com` that is `mapi-project-alex-feng`.
# MAGIC 3. Keep the defaults and create it. You get a `production` branch, a `databricks_postgres` database and a read-write compute.
# MAGIC
# MAGIC Then go to `01_seed_lakebase_project`. **Only run this notebook if you want the project created for you.**

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2"
# MAGIC %restart_python

# COMMAND ----------

# ---------------------------------------------------------------------------------------------
# YOUR INPUTS
# ---------------------------------------------------------------------------------------------
PROJECT_NAME = ""  # Lakebase project ID to create. "" = mapi-project-<your username>

# COMMAND ----------

# MAGIC %run ./_lakebase_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.1 Create the project (or reuse it)
# MAGIC Creating a project also creates its `production` branch, the `databricks_postgres` database and a read-write compute
# MAGIC that autoscales and scales to zero when idle. It takes about a minute. If the project already exists, it is reused unchanged.
# MAGIC If it was deleted from the Lakebase UI in the last 7 days, it is restored instead (a deleted project keeps its ID until it is purged).

# COMMAND ----------

project = find_project(PROJECT_NAME)
if project:
    print(f"Reusing {project.name}")
else:
    if not re.fullmatch(r"[a-z][a-z0-9-]{1,61}[a-z0-9]", PROJECT_NAME):
        raise ValueError(f"'{PROJECT_NAME}' is not a valid project ID: 3-63 lowercase letters, digits or hyphens, starting with a letter.")
    try:  # a project deleted from the Lakebase UI keeps its ID for 7 days
        deleted = w.postgres.get_project(name=f"projects/{PROJECT_NAME}")
    except NotFound:
        deleted = None
    if deleted and deleted.delete_time:
        print(f"projects/{PROJECT_NAME} was deleted but is still recoverable. Restoring it, with its data...")
        w.postgres.undelete_project(name=deleted.name).wait()
        print(f"Restored projects/{PROJECT_NAME}")
    else:
        print(f"Creating projects/{PROJECT_NAME} (about a minute)...")
        w.postgres.create_project(
            project_id=PROJECT_NAME,
            project=postgres.Project(spec=postgres.ProjectSpec(display_name=PROJECT_NAME)),
        ).wait()
        print(f"Created projects/{PROJECT_NAME}")

target = lakebase_target(PROJECT_NAME, "production")
print(json.dumps({k: target[k] for k in ("project", "branch", "database", "endpoint", "host")}, indent=2))
print(f"Compute autoscales between {target['min_cu']} and {target['max_cu']} CU")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Done
# MAGIC Open **Lakebase** from the app switcher to see your project, then go to **`01_seed_lakebase_project`**
# MAGIC (leave `PROJECT_NAME = ""` there if you kept the default name).
