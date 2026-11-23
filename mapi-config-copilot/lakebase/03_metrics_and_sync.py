# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Part 2 - Step 7: metrics in Delta, synced back to Lakebase for the app (~5 min)
# MAGIC
# MAGIC The app writes rows to Lakebase. Analytics belong in the lakehouse. A **synced table** brings the answer back to the app:
# MAGIC
# MAGIC ```
# MAGIC  MAPI app --writes--> Lakebase   mapi.mapi_rows, mapi.change_log
# MAGIC                          |  read the current rows
# MAGIC                          v
# MAGIC  Spark aggregates --> Delta     <catalog>.mapi_lakebase_<user>.mapi_metrics
# MAGIC                          |  Lakebase synced table: a managed pipeline copies Delta into Postgres
# MAGIC                          v
# MAGIC  MAPI app <--reads--- Lakebase   mapi_lakebase_<user>.mapi_metrics_synced   (Metrics tab)
# MAGIC ```
# MAGIC
# MAGIC Until the synced table exists, the app's **Metrics** tab says *Coming soon...*. Once it is there, the tab draws it.
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
BRANCH = "production"  # Branch the app uses
CATALOG = "databricks_showcase_2026"  # Catalog part 1 used; your metrics schema is created here too

# COMMAND ----------

# MAGIC %run ./_lakebase_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Read the current rows from Lakebase
# MAGIC Everything the app has written so far, including your edits from step 4. (If you registered the catalog in step 3, the same rows
# MAGIC are at `mapi_postgres_<user>.mapi.mapi_rows`.)

# COMMAND ----------

target = lakebase_target(PROJECT_NAME, BRANCH)
SCHEMAS = {
    "mapi_rows": "row_id STRING, brand STRING, product STRING, campaign STRING, text_asset_id STRING, primary_copy STRING, "
    "landing_url STRING, status STRING, approved_hash STRING, approved_by STRING, approved_at TIMESTAMP, last_edited_by STRING, "
    "updated_at TIMESTAMP, row_version INT, last_uploaded_at TIMESTAMP",
    "change_log": "change_id STRING, row_id STRING, field STRING, old_value STRING, new_value STRING, changed_by STRING, "
    "changed_at TIMESTAMP, reason STRING, source STRING",
}


def read_lakebase():
    """Load the app's tables from Postgres into Spark temp views lakebase_mapi_rows and lakebase_change_log."""
    with pg_connect(target) as conn:
        for table, ddl in SCHEMAS.items():
            cols = ", ".join(c.split()[0] for c in ddl.split(", "))
            rows = conn.execute(f"SELECT {cols} FROM {PG_SCHEMA}.{table}").fetchall()
            spark.createDataFrame(rows, ddl).createOrReplaceTempView(f"lakebase_{table}")
            print(f"{PG_SCHEMA}.{table}: {len(rows)} rows")


read_lakebase()
display(spark.sql("SELECT status, count(*) AS row_count FROM lakebase_mapi_rows GROUP BY 1 ORDER BY 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Aggregate in Delta
# MAGIC One row per **brand and campaign**: row counts by status, rows with compliance issues, edits by people and by the assistant,
# MAGIC approvals, and the average hours from a row's last edit to its approval. The issue checks join the Lakebase rows with the Delta
# MAGIC `brand_product_master` table from part 1.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METRICS_FQ_SCHEMA} COMMENT 'MAPI Lakebase lab (part 2) for {USER_EMAIL}. Safe to delete.'")

METRICS_SQL = f"""
CREATE OR REPLACE TABLE {METRICS_DELTA_TABLE}
COMMENT 'MAPI metrics per brand and campaign, aggregated from Lakebase ({target["project_id"]}). Synced to Lakebase as {APP_METRICS_TABLE}.'
AS
WITH domains AS (
  SELECT brand, first(allowed_url_domain) AS domain FROM {FQ_SCHEMA}.brand_product_master GROUP BY brand
),
row_state AS (
  SELECT r.row_id, r.brand, r.campaign, r.status, r.last_uploaded_at,
    r.status <> 'archived' AND (
      m.product IS NULL
      OR NOT contains(lower(r.landing_url), coalesce(d.domain, '~'))
      OR NOT contains(lower(r.primary_copy), lower(r.brand))
      OR (r.status = 'approved' AND r.approved_hash <> sha2(concat_ws('|', r.brand, r.product, r.primary_copy, r.landing_url, r.text_asset_id), 256))
    ) AS has_issue
  FROM lakebase_mapi_rows r
  LEFT JOIN {FQ_SCHEMA}.brand_product_master m ON r.brand = m.brand AND r.product = m.product
  LEFT JOIN domains d ON r.brand = d.brand
),
events AS (
  SELECT s.brand, s.campaign, l.field, l.new_value, l.source, l.changed_at,
    CASE WHEN l.field = 'status' AND l.new_value = 'approved' THEN
      CAST(unix_timestamp(l.changed_at) - unix_timestamp(max(CASE WHEN l.field <> 'status' THEN l.changed_at END)
        OVER (PARTITION BY l.row_id ORDER BY l.changed_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS DOUBLE) / 3600
    END AS hours_to_approve
  FROM lakebase_change_log l JOIN row_state s ON l.row_id = s.row_id
),
activity AS (
  SELECT brand, campaign,
    count_if(field <> 'status') AS edits,
    count_if(field <> 'status' AND source <> 'assistant') AS people_edits,
    count_if(field <> 'status' AND source = 'assistant') AS assistant_edits,
    count_if(field = 'status' AND new_value = 'approved') AS approvals,
    avg(hours_to_approve) AS avg_hours_to_approve,
    max(changed_at) AS last_activity_at
  FROM events GROUP BY brand, campaign
),
state AS (
  SELECT brand, campaign,
    count(*) AS total_rows,
    count_if(status = 'approved') AS approved_rows,
    count_if(status = 'draft') AS draft_rows,
    count_if(status = 'archived') AS archived_rows,
    count_if(last_uploaded_at IS NOT NULL) AS uploaded_rows,
    count_if(has_issue) AS issue_rows
  FROM row_state GROUP BY brand, campaign
)
SELECT s.brand, s.campaign, to_date(concat(substr(s.campaign, 1, 7), '-01')) AS campaign_month,
  s.total_rows, s.approved_rows, s.draft_rows, s.archived_rows, s.uploaded_rows, s.issue_rows,
  coalesce(a.edits, 0) AS edits, coalesce(a.people_edits, 0) AS people_edits, coalesce(a.assistant_edits, 0) AS assistant_edits,
  coalesce(a.approvals, 0) AS approvals, round(a.avg_hours_to_approve, 2) AS avg_hours_to_approve,
  a.last_activity_at, current_timestamp() AS refreshed_at
FROM state s LEFT JOIN activity a ON s.brand = a.brand AND s.campaign = a.campaign
"""


def build_metrics():
    spark.sql(METRICS_SQL)
    return spark.table(METRICS_DELTA_TABLE)


metrics = build_metrics()
print(f"{metrics.count()} rows in {METRICS_DELTA_TABLE}")
display(metrics.orderBy("campaign_month", "brand"))
show_link("Metrics table (Catalog Explorer)", catalog_explorer_url(LAB_CATALOG, METRICS_SCHEMA, "mapi_metrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Sync it to Lakebase (reverse ETL)
# MAGIC A **synced table** keeps a read-only Postgres copy of a Unity Catalog table, maintained by a managed pipeline.
# MAGIC It uses **snapshot** mode: the table is small and rebuilt on every refresh, so a full copy each time is the cheap option
# MAGIC (triggered and continuous modes stream only the changes and need Change Data Feed on the source).
# MAGIC
# MAGIC | | Name |
# MAGIC |---|---|
# MAGIC | Source (Delta) | `<catalog>.mapi_lakebase_<user>.mapi_metrics` |
# MAGIC | Synced table (Unity Catalog) | `<catalog>.mapi_lakebase_<user>.mapi_metrics_synced` |
# MAGIC | Postgres table the app reads | `mapi_lakebase_<user>.mapi_metrics_synced` in `databricks_postgres` |
# MAGIC
# MAGIC The first sync starts a pipeline (a minute or two). The cell waits for it.

# COMMAND ----------

SyncPolicy = postgres.SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy
READY = {"SYNCED_TABLE_ONLINE", "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE", "SYNCED_TABLE_ONLINE_TRIGGERED_UPDATE", "SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE"}


def synced_table():
    try:
        return w.postgres.get_synced_table(name=f"synced_tables/{SYNCED_TABLE}")
    except NotFound:
        return None


def last_sync_end(status):
    end = status.last_sync.sync_end_time if status and status.last_sync else None
    return end.ToDatetime(timezone.utc) if end else None


def wait_for_sync(since=None, timeout_s=1200, poll_s=15):
    """Wait until the synced table is online and (if since is given) has finished a sync after that time."""
    start = time.time()
    while time.time() - start < timeout_s:
        status = (synced_table() or postgres.SyncedTable()).status
        state = status.detailed_state.value if status and status.detailed_state else "PENDING"
        last = last_sync_end(status)
        print(f"[{int(time.time() - start)}s] {state}" + (f", last sync finished {last:%H:%M:%S} UTC" if last else ""))
        if "FAILED" in state:
            raise RuntimeError(f"Sync failed: {status.message}")
        if state in READY and last and (since is None or last >= since):
            return status
        time.sleep(poll_s)
    raise TimeoutError("The sync is still running. Re-run this cell in a minute.")


if synced_table():
    print(f"Synced table {SYNCED_TABLE} already exists.")
else:
    print(f"Creating synced table {SYNCED_TABLE} from {METRICS_DELTA_TABLE}...")
    w.postgres.create_synced_table(
        synced_table_id=SYNCED_TABLE,
        synced_table=postgres.SyncedTable(
            spec=postgres.SyncedTableSyncedTableSpec(
                source_table_full_name=METRICS_DELTA_TABLE,
                primary_key_columns=["brand", "campaign"],
                scheduling_policy=SyncPolicy.SNAPSHOT,
                branch=target["branch"],
                postgres_database=PG_DATABASE,
                create_database_objects_if_missing=True,
                new_pipeline_spec=postgres.NewPipelineSpec(storage_catalog=LAB_CATALOG, storage_schema=METRICS_SCHEMA),
            )
        ),
    ).wait()
status = wait_for_sync()
print(f"Synced into Postgres {APP_METRICS_TABLE}. Pipeline: {status.pipeline_id}")
show_link("Synced table (Catalog Explorer)", catalog_explorer_url(LAB_CATALOG, METRICS_SCHEMA, "mapi_metrics_synced"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 Let the app read it
# MAGIC In Postgres the synced table is a normal (read-only) table, so the app's service principal needs `SELECT` on it, like any other table.

# COMMAND ----------

sp = wait_for_app_sp()


def grant_metrics_to_app():
    with pg_connect(target) as conn:
        grant_schema(conn, METRICS_SCHEMA, sp, privileges="SELECT")
        return pg_query(conn, f"SELECT brand, sum(total_rows) AS total_rows, sum(issue_rows) AS issue_rows, sum(approvals) AS approvals "
                              f"FROM {APP_METRICS_TABLE} GROUP BY 1 ORDER BY 1")


display(grant_metrics_to_app())
print(f"App service principal {sp} can now read {APP_METRICS_TABLE}")
show_link("MAPI app (open)", app_open_url())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.5 Look at the Metrics tab
# MAGIC Reload the app and open **Metrics**. *Coming soon...* is gone: the app found its synced table and draws it.
# MAGIC
# MAGIC **Notice:** the app runs no aggregation. It reads ~50 pre-computed rows from Postgres, which is what Lakebase is good at.
# MAGIC The heavy lifting ran in Spark, in the lakehouse.
# MAGIC
# MAGIC ### 7.6 Close the loop
# MAGIC Make a few edits in the app (or ask the assistant for a bulk change and accept it), then run the next cell. It reads Lakebase again,
# MAGIC rebuilds the Delta table and refreshes the synced table. The Metrics tab shows your changes on the next reload.

# COMMAND ----------

refresh_from = datetime.now(timezone.utc)
read_lakebase()
metrics = build_metrics()
w.pipelines.start_update(pipeline_id=synced_table().status.pipeline_id)
wait_for_sync(since=refresh_from)
display(grant_metrics_to_app())  # re-grant in case the refresh replaced the Postgres table
print("Reload the Metrics tab in the app.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Done with part 2?** Run `99_cleanup` (it also switches the app back to PGlite), then `../99_Cleanup` for part 1.
