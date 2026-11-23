# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase config (shared by every notebook in this folder)
# MAGIC
# MAGIC **You do not need to edit or run this notebook directly.** Each notebook in `lakebase/` sets its inputs
# MAGIC (`PROJECT_NAME`, `BRANCH`, `CATALOG`, ...) in its first code cell, then pulls this in with `%run ./_lakebase_config`.
# MAGIC
# MAGIC It runs the part 1 config (`../_lab_config`: your user, schema and apps) and adds the Lakebase names and helpers.
# MAGIC
# MAGIC | Resource | Name |
# MAGIC |---|---|
# MAGIC | Lakebase project | `mapi-project-<user>` (or the project you created by hand) |
# MAGIC | App tables in Postgres | schema `mapi` in database `databricks_postgres` |
# MAGIC | Unity Catalog catalog for your Lakebase database (step 3, by hand) | `mapi_postgres_<user>` |
# MAGIC | Delta metrics table | `<catalog>.mapi_lakebase_<user>.mapi_metrics` |
# MAGIC | Synced table | `<catalog>.mapi_lakebase_<user>.mapi_metrics_synced`, in Postgres `mapi_lakebase_<user>.mapi_metrics_synced` |

# COMMAND ----------

# MAGIC %run ../_lab_config

# COMMAND ----------

import re
import time

import psycopg
from databricks.sdk.errors import NotFound
from psycopg import sql

try:
    from databricks.sdk.service import postgres
except ImportError as e:
    raise ImportError(
        "This notebook needs serverless environment version 6 or later: open the Environment side panel and pick it. "
        "Older environments ship a Databricks SDK without the Lakebase API."
    ) from e

# Lakebase ids allow lowercase letters, digits and hyphens: every other character of the part of your email before @ becomes "-".
USER_SUFFIX = re.sub(r"[^a-z0-9-]", "-", USER_EMAIL.split("@", 1)[0].lower()).strip("-")
DEFAULT_PROJECT_NAME = f"mapi-project-{USER_SUFFIX}"[:63].rstrip("-")

# Inputs from the calling notebook (an empty string means "use the default").
PROJECT_NAME = (globals().get("PROJECT_NAME") or "").strip() or DEFAULT_PROJECT_NAME
BRANCH = (globals().get("BRANCH") or "").strip() or "production"
LAKEBASE_CATALOG = (globals().get("LAKEBASE_CATALOG") or "").strip() or f"mapi_postgres_{USER_SLUG}"

PG_DATABASE = "databricks_postgres"  # the database every new project starts with
PG_SCHEMA = "mapi"  # same schema name the app uses in PGlite, so the app's SQL does not change

METRICS_SCHEMA = f"mapi_lakebase_{USER_SLUG}"
METRICS_FQ_SCHEMA = f"{LAB_CATALOG}.{METRICS_SCHEMA}"
METRICS_DELTA_TABLE = f"{METRICS_FQ_SCHEMA}.mapi_metrics"
# A synced table lands in Postgres under its Unity Catalog schema and table names.
SYNCED_TABLE = f"{METRICS_FQ_SCHEMA}.mapi_metrics_synced"
APP_METRICS_TABLE = f"{METRICS_SCHEMA}.mapi_metrics_synced"  # what the app's Metrics tab reads

# COMMAND ----------

# ---- Find your project, branch, database and compute ----


def find_project(name=None):
    """Accepts a project ID, projects/<id>, the name you typed in the Lakebase UI, or the project UUID."""
    key = (name or PROJECT_NAME).strip().removeprefix("projects/")
    if re.fullmatch(r"[a-z][a-z0-9-]{0,62}", key):
        try:
            project = w.postgres.get_project(name=f"projects/{key}")
            if not project.delete_time:
                return project
        except NotFound:
            pass
    for project in w.postgres.list_projects():
        names = {
            project.name.removeprefix("projects/"),
            project.uid,
            project.status.display_name if project.status else None,
            project.spec.display_name if project.spec else None,
        }
        if key in names:
            return project
    return None


def _endpoint_type(e):
    return (e.status.endpoint_type if e.status else None) or (e.spec.endpoint_type if e.spec else None)


def lakebase_target(project_name=None, branch=None):
    """Everything needed to connect: resource paths, the Postgres database name and the endpoint hostname."""
    project = find_project(project_name)
    if project is None:
        raise RuntimeError(
            f"No Lakebase project called '{project_name or PROJECT_NAME}'. Create it by hand (see 00_optional_lakebase_project_setup) "
            "or run that notebook, then set PROJECT_NAME at the top of this notebook to its name."
        )
    branch_path = f"{project.name}/branches/{branch or BRANCH}"
    w.postgres.get_branch(name=branch_path)
    databases = [d for d in w.postgres.list_databases(parent=branch_path) if d.status and d.status.postgres_database == PG_DATABASE]
    if not databases:
        raise RuntimeError(f"Database {PG_DATABASE} not found on {branch_path}.")
    endpoints = [e for e in w.postgres.list_endpoints(parent=branch_path) if _endpoint_type(e) == postgres.EndpointType.ENDPOINT_TYPE_READ_WRITE]
    if not endpoints:
        raise RuntimeError(f"{branch_path} has no read-write compute yet. Wait for it in the Lakebase UI, then re-run.")
    endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
    host = endpoint.status.hosts.host if endpoint.status and endpoint.status.hosts else None
    if not host:
        raise RuntimeError(f"{endpoint.name} has no hostname yet. Wait a minute and re-run.")
    return {
        "project": project.name,
        "project_id": project.name.removeprefix("projects/"),
        "branch": branch_path,
        "branch_id": branch_path.rsplit("/", 1)[-1],
        "database": databases[0].name,
        "database_name": PG_DATABASE,
        "endpoint": endpoint.name,
        "host": host,
        "min_cu": endpoint.status.autoscaling_limit_min_cu,
        "max_cu": endpoint.status.autoscaling_limit_max_cu,
    }


def pg_connect(target, attempts=5):
    """Connect as you, with a short-lived OAuth token as the password. Retries while the compute wakes up."""
    for attempt in range(attempts):
        token = w.postgres.generate_database_credential(endpoint=target["endpoint"]).token
        try:
            return psycopg.connect(
                host=target["host"],
                port=5432,
                dbname=target["database_name"],
                user=USER_EMAIL,
                password=token,
                sslmode="require",
                connect_timeout=20,
                autocommit=True,
            )
        except psycopg.OperationalError:
            if attempt == attempts - 1:
                raise
            time.sleep(2**attempt)


def pg_query(conn, query, params=None):
    import pandas as pd

    with conn.cursor() as cur:
        cur.execute(query, params)
        return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])


# COMMAND ----------

# ---- The app's tables in Postgres ----
# Same columns as the PGlite tables in app/app.py, plus the rules PGlite left to the app: a status check, and a trigger that
# bumps row_version and sends an edited approved row back to draft. That trigger is the database-level fix for the 5 November
# incident: an approved row cannot change brand or product and stay approved.

MAPI_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA};

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.mapi_rows (
  row_id TEXT PRIMARY KEY, brand TEXT NOT NULL, product TEXT NOT NULL, campaign TEXT NOT NULL, text_asset_id TEXT NOT NULL,
  primary_copy TEXT NOT NULL, landing_url TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('draft', 'approved', 'archived')),
  approved_hash TEXT, approved_by TEXT, approved_at TIMESTAMPTZ, last_edited_by TEXT, updated_at TIMESTAMPTZ,
  row_version INT NOT NULL DEFAULT 1, last_uploaded_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.change_log (
  change_id TEXT PRIMARY KEY, row_id TEXT, field TEXT, old_value TEXT, new_value TEXT,
  changed_by TEXT, changed_at TIMESTAMPTZ, reason TEXT, source TEXT
);
CREATE INDEX IF NOT EXISTS change_log_row_time ON {PG_SCHEMA}.change_log (row_id, changed_at DESC);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.upload_controls (
  control TEXT PRIMARY KEY, value TEXT, updated_by TEXT, updated_at TIMESTAMPTZ
);

CREATE OR REPLACE FUNCTION {PG_SCHEMA}.content_hash(brand TEXT, product TEXT, primary_copy TEXT, landing_url TEXT, text_asset_id TEXT)
RETURNS TEXT LANGUAGE sql IMMUTABLE AS $$
  SELECT encode(sha256(convert_to(concat_ws('|', brand, product, primary_copy, landing_url, text_asset_id), 'UTF8')), 'hex')
$$;

CREATE OR REPLACE FUNCTION {PG_SCHEMA}.guard_row_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.row_version := OLD.row_version + 1;
  NEW.updated_at := now();
  -- Approval is bound to content: changing an approved row sends it back to draft.
  IF (NEW.brand, NEW.product, NEW.campaign, NEW.text_asset_id, NEW.primary_copy, NEW.landing_url)
     IS DISTINCT FROM (OLD.brand, OLD.product, OLD.campaign, OLD.text_asset_id, OLD.primary_copy, OLD.landing_url) THEN
    IF NEW.status = 'approved' THEN
      NEW.status := 'draft';
    END IF;
    NEW.approved_hash := NULL;
    NEW.approved_by := NULL;
    NEW.approved_at := NULL;
  END IF;
  -- A new approval must record the hash of what it approves, and cannot come from the row's last editor.
  IF NEW.status = 'approved' AND OLD.status IS DISTINCT FROM 'approved' THEN
    IF NEW.approved_hash IS DISTINCT FROM {PG_SCHEMA}.content_hash(NEW.brand, NEW.product, NEW.primary_copy, NEW.landing_url, NEW.text_asset_id) THEN
      RAISE EXCEPTION 'approval hash does not match the row content';
    END IF;
    IF NEW.approved_by = NEW.last_edited_by THEN
      RAISE EXCEPTION 'the last editor of a row cannot approve it';
    END IF;
  END IF;
  RETURN NEW;
END $$;

CREATE OR REPLACE TRIGGER guard_row_update BEFORE UPDATE ON {PG_SCHEMA}.mapi_rows
  FOR EACH ROW EXECUTE FUNCTION {PG_SCHEMA}.guard_row_update();
"""


def create_mapi_tables(conn):
    conn.execute(MAPI_DDL)


def ensure_app_role(target, sp_client_id):
    """Binding Lakebase to the app normally creates the app's Postgres login. Create it here if it is missing."""
    with pg_connect(target) as conn:
        if conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (sp_client_id,)).fetchone():
            return
    print(f"Creating the Postgres role for service principal {sp_client_id} on {target['branch']}...")
    w.postgres.create_role(
        parent=target["branch"],
        role=postgres.Role(
            spec=postgres.RoleRoleSpec(
                postgres_role=sp_client_id,
                identity_type=postgres.RoleIdentityType.SERVICE_PRINCIPAL,
                auth_method=postgres.RoleAuthMethod.LAKEBASE_OAUTH_V1,
            )
        ),
    ).wait()


def grant_schema(conn, schema, role, privileges="SELECT, INSERT, UPDATE, DELETE"):
    """USAGE on the schema plus table privileges, now and for tables you create in it later."""
    ident = sql.Identifier
    conn.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(ident(schema), ident(role)))
    conn.execute(sql.SQL("GRANT " + privileges + " ON ALL TABLES IN SCHEMA {} TO {}").format(ident(schema), ident(role)))
    conn.execute(
        sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT " + privileges + " ON TABLES TO {}").format(ident(schema), ident(role))
    )


def lakebase_app_env(target):
    """Feature flag plus the Lakebase settings the app needs. Everything else stays as it was in part 1."""
    return {
        "USE_LAKEBASE": "true",
        "LAKEBASE_PROJECT": target["project_id"],
        "LAKEBASE_BRANCH": target["branch_id"],
        "LAKEBASE_ENDPOINT": target["endpoint"],  # fallback if the app cannot look the project up itself
        "LAKEBASE_HOST": target["host"],
        "LAKEBASE_DB": target["database_name"],
        "LAKEBASE_SCHEMA": PG_SCHEMA,
        "METRICS_TABLE": APP_METRICS_TABLE,
    }


def catalog_explorer_url(catalog, schema=None, table=None):
    return f"{WORKSPACE_HOST}/explore/data/" + "/".join(p for p in (catalog, schema, table) if p)


print(f"Lakebase project : {PROJECT_NAME}   (default for you: {DEFAULT_PROJECT_NAME})")
print(f"Branch           : {BRANCH}")
print(f"Catalog          : {LAB_CATALOG}")
