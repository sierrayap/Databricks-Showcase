# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Part 2 - Step 1: put the MAPI data in Lakebase (~3 min)
# MAGIC
# MAGIC In part 1 the app kept its rows in **PGlite**, an in-memory Postgres that forgets every edit when the app restarts.
# MAGIC Here you create the same tables in your **Lakebase** project and fill them, so the app can switch over in step 4.
# MAGIC
# MAGIC | Table (Postgres schema `mapi`) | What goes in |
# MAGIC |---|---|
# MAGIC | `mapi_rows` | The 126 part 1 rows from Unity Catalog, unchanged (same row IDs the assistant sees), plus ~300 rows from nine earlier 2026 campaigns |
# MAGIC | `change_log` | About 1,500 edits, approvals and archives across the year, by editors, approvers and the assistant |
# MAGIC | `upload_controls` | The `uploads_paused` switch |
# MAGIC
# MAGIC The history is what makes the metrics in step 7 worth looking at. It also contains the **5 November incident** on `R-0068`.
# MAGIC
# MAGIC **Action:** set your inputs, then **Run all**. Re-running is safe: rows that are already there (including your edits) are kept.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2"
# MAGIC %restart_python

# COMMAND ----------

# ---------------------------------------------------------------------------------------------
# YOUR INPUTS
# ---------------------------------------------------------------------------------------------
PROJECT_NAME = ""  # Your Lakebase project ID or name. "" = mapi-project-<your username>
BRANCH = "production"  # Branch to seed
CATALOG = "databricks_showcase_2026"  # Catalog part 1 used (your part 1 tables are read from here)

# COMMAND ----------

# MAGIC %run ./_lakebase_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Find your project and the part 1 tables

# COMMAND ----------

target = lakebase_target(PROJECT_NAME, BRANCH)
print(json.dumps({k: target[k] for k in ("project", "branch", "database", "endpoint", "host")}, indent=2))

missing = [t for t in ("mapi_rows", "change_log", "upload_controls", "brand_product_master") if not spark.catalog.tableExists(f"{FQ_SCHEMA}.{t}")]
if missing:
    raise RuntimeError(f"{FQ_SCHEMA} is missing {missing}. Run part 1 (../01_Setup_and_Kickoff) first, with the same CATALOG.")
print(f"Part 1 tables found in {FQ_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Create the tables
# MAGIC The same columns the app created in PGlite, plus two rules that PGlite left to the app code. Lakebase is real Postgres, so the
# MAGIC database can enforce them for every client:
# MAGIC * `status` can only be `draft`, `approved` or `archived`.
# MAGIC * A trigger bumps `row_version` on every update and **sends an approved row back to draft when its content changes**.
# MAGIC   It also refuses an approval whose hash does not match the content, or that comes from the row's last editor.
# MAGIC   With this in place, the 5 November edit (brand switched on an approved row) could not have stayed approved.

# COMMAND ----------

with pg_connect(target) as conn:
    create_mapi_tables(conn)
    tables = pg_query(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY 1", (PG_SCHEMA,))
print(f"Schema {PG_SCHEMA} in {PG_DATABASE}: {', '.join(tables.table_name)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Build the seed data
# MAGIC The part 1 rows come straight from Unity Catalog. The history is generated with a fixed seed, so everyone gets the same data:
# MAGIC campaigns `2026-01` to `2026-09` (archived once they ended), and edits, approvals and archives for every row. A few patterns are
# MAGIC built in for step 7: approvals get faster through the year, `My Comparison Life` approvals are the slowest, and the assistant's share
# MAGIC of edits grows from its pilot in August.

# COMMAND ----------

import random
from datetime import datetime, timedelta, timezone

rng = random.Random(2026)
UTC = timezone.utc
ROW_COLS = ["row_id", "brand", "product", "campaign", "text_asset_id", "primary_copy", "landing_url", "status", "approved_hash",
            "approved_by", "approved_at", "last_edited_by", "updated_at", "row_version", "last_uploaded_at"]
LOG_COLS = ["change_id", "row_id", "field", "old_value", "new_value", "changed_by", "changed_at", "reason", "source"]

EDITORS = ["rachel.mex@example.com", "hannah.mex@example.com", "sam.mex@example.com"]
APPROVERS = ["amadea.dp@example.com", "maria.dp@example.com"]
HISTORY_CAMPAIGNS = ["2026-01-summer", "2026-02-back-to-school", "2026-03-autumn", "2026-04-easter", "2026-05-mothers-day",
                     "2026-06-eofy", "2026-07-winter", "2026-08-always-on", "2026-09-fathers-day"]
HOURS_TO_APPROVE = {"Real Funeral": 5, "Real Insurance": 12, "OneChoice": 9, "My Comparison Life": 28}
EDITOR_REASONS = {
    "landing_url": ["Fix tracking parameters", "Point at the new quote page", "Remove an old UTM tag"],
    "primary_copy": ["Copy review feedback", "Compliance wording update"],
    "campaign": ["Move to the right campaign"],
}


def utc(ts):
    return ts.replace(tzinfo=UTC) if ts is not None and ts.tzinfo is None else ts


def content_hash(brand, product, primary_copy, landing_url, text_asset_id):
    return hashlib.sha256("|".join([brand, product, primary_copy, landing_url, text_asset_id]).encode()).hexdigest()


def month_of(campaign):
    return int(campaign[5:7])


def approval_hours(brand, campaign):
    """Approvals get faster through the year, and some brands are slower than others."""
    mean = HOURS_TO_APPROVE[brand] * max(0.6, 1.5 - 0.08 * (month_of(campaign) - 1))
    return max(0.3, rng.gauss(mean, mean * 0.45))


def assistant_share(campaign):
    return {8: 0.15, 9: 0.3, 10: 0.45, 11: 0.55, 12: 0.6}.get(month_of(campaign), 0.0)


def earlier_values(field, final, row, n):
    """n older versions of a field, oldest first, so the log chains to the value the row has now."""
    if field == "landing_url":
        return [f"{final}&utm_content=v{i}" for i in range(n, 0, -1)]
    if field == "primary_copy":
        return [final.split(". ")[0] + "."] * n
    return [f"{row['campaign'][:7]}-tbc"] * n


def edit_events(row, n, end, earliest):
    """n edits between earliest and end, the last one at end. Returns log entries, oldest first."""
    fields = [rng.choices(["landing_url", "primary_copy", "campaign"], weights=[6, 2, 1])[0] for _ in range(n)]
    for f in ("primary_copy", "campaign"):  # at most one copy edit and one campaign move per row
        while fields.count(f) > 1:
            fields[fields.index(f)] = "landing_url"
    times = sorted(earliest + (end - earliest) * rng.random() for _ in range(n - 1)) + [end] if n else []
    olds = {f: earlier_values(f, row[f], row, fields.count(f)) for f in set(fields)}
    seen = {f: 0 for f in fields}
    events = []
    for f, ts in zip(fields, times):
        chain = olds[f] + [row[f]]
        old, new = chain[seen[f]], chain[seen[f] + 1]
        seen[f] += 1
        by_assistant = f != "primary_copy" and rng.random() < assistant_share(row["campaign"])
        if by_assistant:
            who, source = rng.choice(EDITORS), "assistant"
            reason = f"Change the {f.replace('_', ' ')} for the {row['brand']} {row['text_asset_id']} rows in {row['campaign']}"
        else:
            who, source, reason = rng.choice(EDITORS), "editor", rng.choice(EDITOR_REASONS[f])
        events.append({"field": f, "old_value": old, "new_value": new, "changed_by": who, "changed_at": ts, "reason": reason, "source": source})
    return events


def approval_event(approver, ts):
    return {"field": "status", "old_value": "draft", "new_value": "approved", "changed_by": approver, "changed_at": ts,
            "reason": "approved in approval queue", "source": "approver"}


# ---- part 1 rows, unchanged ----
uc_rows = [{c: (utc(v) if isinstance(v, datetime) else v) for c, v in r.asDict().items()} for r in spark.table(f"{FQ_SCHEMA}.mapi_rows").orderBy("row_id").collect()]
uc_log = [{c: (utc(v) if isinstance(v, datetime) else v) for c, v in r.asDict().items()} for r in spark.table(f"{FQ_SCHEMA}.change_log").collect()]
uc_controls = [{c: (utc(v) if isinstance(v, datetime) else v) for c, v in r.asDict().items()} for r in spark.table(f"{FQ_SCHEMA}.upload_controls").collect()]
master = spark.table(f"{FQ_SCHEMA}.brand_product_master").orderBy("brand", "product").collect()
domain = {m.brand: m.allowed_url_domain for m in master}
copy_for = {(r["brand"], r["product"]): r["primary_copy"] for r in uc_rows if r["primary_copy"] and r["brand"] in r["primary_copy"]}

rows = [dict(r) for r in uc_rows]
log = list(uc_log)


def add_log(row_id, events):
    for e in events:
        log.append({"change_id": f"hist-{row_id}-{len([x for x in log if x['row_id'] == row_id]):02d}", "row_id": row_id, **e})


# History for the part 1 rows: edits before the current state, approvals at the recorded approval time.
for r in rows:
    if r["row_id"] == "R-0068":
        continue
    if r["status"] == "approved":
        approved_at = r["approved_at"]
        last_edit = approved_at - timedelta(hours=approval_hours(r["brand"], r["campaign"]))
        add_log(r["row_id"], edit_events(r, rng.choice([0, 1, 1, 2, 3]), last_edit, last_edit - timedelta(days=12)))
        add_log(r["row_id"], [approval_event(r["approved_by"], approved_at)])
    else:  # drafts already have their part 1 "waiting for approval" entry at updated_at
        add_log(r["row_id"], edit_events(r, rng.choice([1, 1, 2, 3]), r["updated_at"] - timedelta(hours=2), r["updated_at"] - timedelta(days=14)))

# The 5 November incident: approved as Real Funeral, then re-branded in the shared sheet during testing.
add_log("R-0068", [
    {"field": "landing_url", "old_value": "https://www.realfuneral.example/funeral-cover/2026-11-always-on?asset=t005&utm_content=v1",
     "new_value": "https://www.realfuneral.example/funeral-cover/2026-11-always-on?asset=t005", "changed_by": "rachel.mex@example.com",
     "changed_at": datetime(2026, 10, 29, 16, 40, tzinfo=UTC), "reason": "Remove an old UTM tag", "source": "editor"},
    approval_event("amadea.dp@example.com", datetime(2026, 10, 30, 9, 12, tzinfo=UTC)),
    {"field": "brand", "old_value": "Real Funeral", "new_value": "My Comparison Life", "changed_by": "data.engineer@example.com",
     "changed_at": datetime(2026, 11, 5, 14, 3, tzinfo=UTC), "reason": "Testing in the shared sheet", "source": "sheet"},
    {"field": "product", "old_value": "funeral_cover", "new_value": "life_cover_compare", "changed_by": "data.engineer@example.com",
     "changed_at": datetime(2026, 11, 5, 14, 3, tzinfo=UTC), "reason": "Testing in the shared sheet", "source": "sheet"},
])

# Nine earlier campaigns. Every row was approved (a few were not), uploaded to Meta, and archived when the campaign ended.
next_id = len(rows) + 1
for campaign in HISTORY_CAMPAIGNS:
    month = month_of(campaign)
    starts = datetime(2026, month, 1, tzinfo=UTC)
    ends = datetime(2026, month + 1, 1, tzinfo=UTC)
    for m in master:
        if rng.random() > 0.9:  # not every brand runs every product every month
            continue
        for a in range(1, rng.randint(4, 6) + 1):
            row = {"row_id": f"R-{next_id:04d}", "brand": m.brand, "product": m.product, "campaign": campaign,
                   "text_asset_id": f"t00{a}", "primary_copy": copy_for[(m.brand, m.product)],
                   "landing_url": f"https://www.{domain[m.brand]}/{m.product.replace('_', '-')}/{campaign}?asset=t00{a}"}
            next_id += 1
            created = starts - timedelta(days=rng.randint(8, 20), hours=rng.randint(0, 9))
            last_edit_at = starts - timedelta(days=rng.randint(2, 6), hours=rng.randint(0, 12), minutes=rng.randint(0, 59))
            events = edit_events(row, rng.choices([0, 1, 2, 3, 4, 5], weights=[1, 3, 3, 2, 1, 1])[0], last_edit_at, created)
            last_edit = events[-1]["changed_at"] if events else created
            approver = approved_at = uploaded_at = None
            if rng.random() > 0.06:
                approver = rng.choice(APPROVERS)
                approved_at = last_edit + timedelta(hours=approval_hours(m.brand, campaign))
                events.append(approval_event(approver, approved_at))
                if rng.random() < 0.12:  # changed after approval: back to draft, approved again
                    fix = edit_events(row, 1, approved_at + timedelta(hours=rng.randint(2, 48)), approved_at + timedelta(hours=1))
                    approved_at = fix[0]["changed_at"] + timedelta(hours=approval_hours(m.brand, campaign))
                    events += fix + [approval_event(approver, approved_at)]
                uploaded_at = approved_at + timedelta(hours=rng.randint(1, 20))
            archiver = rng.choice(EDITORS)
            archived_at = ends + timedelta(days=rng.randint(1, 5), hours=rng.randint(8, 17))
            events.append({"field": "status", "old_value": "approved" if approver else "draft", "new_value": "archived",
                           "changed_by": archiver, "changed_at": archived_at, "reason": "Campaign ended", "source": "editor"})
            row.update({"status": "archived",
                        "approved_hash": content_hash(m.brand, m.product, row["primary_copy"], row["landing_url"], row["text_asset_id"]) if approver else None,
                        "approved_by": approver, "approved_at": approved_at, "last_edited_by": archiver, "updated_at": archived_at,
                        "row_version": 1 + len(events), "last_uploaded_at": uploaded_at})
            rows.append(row)
            add_log(row["row_id"], events)

print(f"{len(rows)} rows ({len(uc_rows)} from part 1), {len(log)} change log entries, {len(uc_controls)} upload control")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Load it into Lakebase
# MAGIC Plain `INSERT ... ON CONFLICT DO NOTHING`, in one transaction. Rows that are already there are left alone, so re-running never
# MAGIC overwrites edits made in the app.

# COMMAND ----------


def insert(conn, table, cols, records, key):
    before = conn.execute(f"SELECT count(*) FROM {PG_SCHEMA}.{table}").fetchone()[0]
    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {PG_SCHEMA}.{table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) ON CONFLICT ({key}) DO NOTHING",
            [tuple(r[c] for c in cols) for r in records],
        )
    after = conn.execute(f"SELECT count(*) FROM {PG_SCHEMA}.{table}").fetchone()[0]
    print(f"{PG_SCHEMA}.{table}: {after - before} inserted, {after} rows in total")


with pg_connect(target) as conn:
    with conn.transaction():
        insert(conn, "mapi_rows", ROW_COLS, rows, "row_id")
        insert(conn, "change_log", LOG_COLS, log, "change_id")
        insert(conn, "upload_controls", ["control", "value", "updated_by", "updated_at"], uc_controls, "control")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Check it
# MAGIC The trigger is easy to see working: change an approved row in a transaction you roll back, and watch it fall back to draft.

# COMMAND ----------

with pg_connect(target) as conn:
    display(pg_query(conn, f"SELECT brand, status, count(*) AS row_count FROM {PG_SCHEMA}.mapi_rows GROUP BY 1, 2 ORDER BY 1, 2"))
    print("History of R-0068, the 5 November incident:")
    display(pg_query(conn, f"SELECT changed_at, changed_by, field, old_value, new_value, source FROM {PG_SCHEMA}.change_log WHERE row_id = 'R-0068' ORDER BY changed_at"))

    with conn.transaction(force_rollback=True):
        rid = conn.execute(f"SELECT row_id FROM {PG_SCHEMA}.mapi_rows WHERE status = 'approved' ORDER BY row_id LIMIT 1").fetchone()[0]
        conn.execute(f"UPDATE {PG_SCHEMA}.mapi_rows SET brand = 'My Comparison Life' WHERE row_id = %s", (rid,))
        status, version = conn.execute(f"SELECT status, row_version FROM {PG_SCHEMA}.mapi_rows WHERE row_id = %s", (rid,)).fetchone()
        print(f"Trigger check on {rid}: after a brand change it is '{status}' (row_version {version}). Rolled back, nothing saved.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Query Lakebase from Unity Catalog (by hand, ~3 min)
# MAGIC Register your Lakebase database as a **catalog** in Unity Catalog, then query it like any other table, joined with Delta.
# MAGIC
# MAGIC 1. Open **Catalog** (Catalog Explorer), click **+** then **Create a catalog**.
# MAGIC 2. Name it with the catalog name printed below. Choose type **Lakebase Postgres**, then **Autoscaling**.
# MAGIC 3. Pick your project, the `production` branch and the `databricks_postgres` database. Click **Create**.
# MAGIC 4. Open the **SQL editor**, choose a **serverless** SQL warehouse, and run the queries printed below.
# MAGIC
# MAGIC **Notice:** the catalog is read-only and every query runs against the live Postgres rows. Edits you make in the app show up immediately,
# MAGIC with no copy job.
# MAGIC
# MAGIC Creating a catalog needs `CREATE CATALOG` on the metastore (the presenter grants it to the workshop group beforehand).
# MAGIC If you do not have it, read along: nothing later depends on this catalog. `99_cleanup` deletes it.

# COMMAND ----------

print(f"Catalog name : {LAKEBASE_CATALOG}")
print(f"Project      : {target['project_id']}   branch: {target['branch_id']}   database: {PG_DATABASE}\n")
print(f"""-- 1. Rows by brand and status, straight from Postgres
SELECT brand, status, count(*) AS row_count
FROM {LAKEBASE_CATALOG}.{PG_SCHEMA}.mapi_rows
GROUP BY ALL ORDER BY ALL;

-- 2. Join Lakebase with Delta: rows whose product the brand does not sell (finds R-0068)
SELECT r.row_id, r.brand, r.product, r.status, r.last_edited_by
FROM {LAKEBASE_CATALOG}.{PG_SCHEMA}.mapi_rows r
LEFT JOIN {FQ_SCHEMA}.brand_product_master m ON r.brand = m.brand AND r.product = m.product
WHERE m.product IS NULL;

-- 3. Who changed what, by month
SELECT date_trunc('MONTH', changed_at) AS month, source, count(*) AS changes
FROM {LAKEBASE_CATALOG}.{PG_SCHEMA}.change_log
GROUP BY ALL ORDER BY ALL;""")
show_link("Catalog Explorer", f"{WORKSPACE_HOST}/explore/data")

# COMMAND ----------

# MAGIC %md
# MAGIC **Next:** `02_redeploy_app_with_lakebase`.
