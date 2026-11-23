# Databricks notebook source
# MAGIC %md
# MAGIC # Lab config (shared by every notebook in this lab)
# MAGIC
# MAGIC **You do not need to edit or run this notebook directly** - every lab notebook pulls it in with `%run ./_lab_config`.
# MAGIC
# MAGIC It works out **your** resource names from your login (so 30 people never collide), keeps a registry of
# MAGIC everything you create (so `99_Cleanup` deletes exactly that), and provides small helpers for
# MAGIC Agent Bricks, Unity AI Gateway, Databricks Apps and Lakebase.
# MAGIC
# MAGIC | Resource | Name |
# MAGIC |---|---|
# MAGIC | Schema | `<catalog>.dbx_showcase_<user>_mapi` |
# MAGIC | Agent Bricks custom agent (DurableAgentServer, Databricks App) | `dbxs-ag-<user>-<hash>` |
# MAGIC | AI Gateway model service | `<schema>.dbx_showcase_<user>_mapi_llm` |
# MAGIC | Guardrail function | `<catalog>.mapi_policies.block_ai_ad_copy_<user>` (shared schema) |
# MAGIC | MAPI UI Databricks App | `dbxs-mapi-<user>-<hash>` |
# MAGIC | MLflow experiment | `/Users/<you>/dbx_showcase_<user>_mapi_eval` |

# COMMAND ----------

# Presenter: set these once before the workshop. Participants never change them.
LAB_CATALOG = "databricks_showcase_2026"
LAB_CODE = "mapi"  # unique per showcase so cleanup in one lab never touches another
SUMMARY_MODEL = "system.ai.databricks-claude-sonnet-4-6"  # primary model behind the AI Gateway model service (also used by the agent)
FALLBACK_MODEL = "system.ai.databricks-claude-sonnet-4-5"
LAB_WAREHOUSE_ID = None  # None = auto-pick a serverless SQL warehouse the app can use

# The lakebase/ notebooks set CATALOG in their input cell before running this config; it wins when set.
LAB_CATALOG = globals().get("CATALOG") or LAB_CATALOG

# COMMAND ----------

import hashlib
import json
import re
import time
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
USER_EMAIL = w.current_user.me().user_name
USER_SLUG = re.sub(r"[^a-z0-9]+", "_", USER_EMAIL.split("@")[0].lower()).strip("_")
_HASH4 = hashlib.sha1(USER_EMAIL.lower().encode()).hexdigest()[:4]
DASH_SLUG = USER_SLUG.replace("_", "-")[:14].strip("-")

PREFIX = f"dbx_showcase_{USER_SLUG}_{LAB_CODE}"
SCHEMA = PREFIX
FQ_SCHEMA = f"{LAB_CATALOG}.{SCHEMA}"
VOLUME = "brand_rules"
VOLUME_PATH = f"/Volumes/{LAB_CATALOG}/{SCHEMA}/{VOLUME}"

UC_AGENT_FUNCTIONS = "search_rows,propose_change_set,find_issues,lookup_rules"
MODEL_SERVICE_ID = f"{PREFIX}_llm"
MODEL_SERVICE_FQN = f"{FQ_SCHEMA}.{MODEL_SERVICE_ID}"
# The guardrail lives in a shared policy schema, not in your lab schema: Agent Bricks tools fail
# if any VARIANT-typed function sits in the same schema as them. Each participant gets their own
# function, named with their username, so cleanup can drop exactly theirs.
POLICY_SCHEMA = f"{LAB_CATALOG}.mapi_policies"
GUARDRAIL_FN = f"{POLICY_SCHEMA}.block_ai_ad_copy_{USER_SLUG}"
EXPERIMENT_PATH = f"/Users/{USER_EMAIL}/{PREFIX}_eval"
REGISTRY_TABLE = f"{FQ_SCHEMA}.lab_resources"

# Apps: 2-30 chars, lowercase letters, numbers, hyphens. Lakebase project ids follow the same rule.
APP_NAME = f"dbxs-{LAB_CODE}-{DASH_SLUG}-{_HASH4}"
AGENT_APP_NAME = f"dbxs-ag-{DASH_SLUG}-{_HASH4}"
LAKEBASE_PROJECT = APP_NAME
LAKEBASE_DB = "mapi"
LAKEBASE_PG_SCHEMA = "mapi"
TEST_BRANCH = "test"

WORKSPACE_HOST = w.config.host.rstrip("/")
AI_GATEWAY_BASE_URL = f"{WORKSPACE_HOST}/ai-gateway/mlflow/v1"

# COMMAND ----------


def api(method, path, body=None, query=None):
    return w.api_client.do(method, path, body=body, query=query) or {}


def _err_text(e):
    parts = [str(e), getattr(e, "message", "") or "", getattr(e, "error_code", "") or ""]
    return " ".join(str(p) for p in parts).lower()


_NOT_READY_PHRASES = (
    "still being provisioned",
    "being provisioned",
    "not available yet",
    "not ready",
    "not yet ready",
    "is not yet",
    "provisioning",
    "temporarily unavailable",
    "service unavailable",
    "cold start",
    "scaled to zero",
    "scaling up",
    "is being created",
    "in progress",
    "try again later",
    "try again in",
    "timed out",
    "timeout",
    "connection error",
    "connection reset",
    "connection aborted",
    "bad gateway",
    "gateway time-out",
)
_RETRY_STATUS = {404, 408, 409, 425, 500, 502, 503, 504}


def _still_provisioning(e):
    """True for errors that just mean 'not ready yet' - never for rate limits or real bugs."""
    text = _err_text(e)
    if "rate limit" in text or "too many requests" in text:
        return False
    status = getattr(e, "status_code", None)
    if isinstance(status, int) and status in _RETRY_STATUS:
        return True
    return any(s in text for s in _NOT_READY_PHRASES)


def retry_until_ready(fn, what="resource", timeout_s=600, poll_s=15, retry_if=None):
    """Call fn() until it stops failing with a 'still provisioning / not ready' error."""
    retry_if = retry_if or _still_provisioning
    start = time.time()
    while True:
        try:
            return fn()
        except Exception as e:
            if not retry_if(e) or time.time() - start > timeout_s:
                raise
            print(f"[{int(time.time() - start)}s] {what} is not ready yet, waiting... ({str(e)[:90]})")
            time.sleep(poll_s)


def api_when_ready(method, path, body=None, query=None, timeout_s=900, poll_s=20):
    """api() that waits while a just-created resource is still provisioning."""
    return retry_until_ready(lambda: api(method, path, body=body, query=query), what=path, timeout_s=timeout_s, poll_s=poll_s)


def ensure_examples(resource, labels, timeout_s=900, poll_s=20, soft=True):
    """Create Agent Bricks example questions once the examples API is ready.

    Examples are nice-to-have labels, so with soft=True a timeout prints a note instead of failing the notebook."""
    path = f"/api/2.1/{resource}/examples"
    try:
        existing = {e.get("question") for e in retry_until_ready(lambda: list_items(path, "examples"), what=f"examples for {resource}", timeout_s=timeout_s, poll_s=poll_s)}
        created = 0
        for question, guidelines in labels:
            if question in existing:
                continue
            api_when_ready("POST", path, body={"question": question, "guidelines": guidelines}, timeout_s=timeout_s, poll_s=poll_s)
            created += 1
        print(f"Examples on {resource}: {len(existing)} existing, {created} created")
        return True
    except Exception as e:
        if not soft:
            raise
        print(f"Note: could not register example questions for {resource} yet ({str(e)[:120]}). The lab works without them.")
        return False


_IN_PROGRESS = ("CREATING", "UPDATING", "PROVISIONING", "PENDING", "SYNCING", "INDEXING", "STARTING")
_FAILED = ("FAILED", "ERROR")


def wait_for_endpoint(endpoint_name, timeout_s=1200, poll_s=15, strict=True):
    """Wait until a serving endpoint is READY. Raises on timeout so later cells never run against a half-built endpoint."""
    assert endpoint_name, "No endpoint name yet - run 01_Setup_and_Kickoff first."
    start = time.time()
    last = None
    while time.time() - start < timeout_s:
        try:
            ep = w.serving_endpoints.get(endpoint_name)
            ready = str(ep.state.ready) if ep.state else "UNKNOWN"
            update = str(ep.state.config_update) if ep.state else ""
            if "READY" in ready and "NOT" not in ready and "IN_PROGRESS" not in update:
                print(f"[{int(time.time() - start)}s] {endpoint_name} is READY")
                return True
            last = f"{ready} / {update}"
            print(f"[{int(time.time() - start)}s] {endpoint_name}: {last}")
        except Exception as e:
            last = str(e)[:120]
            print(f"[{int(time.time() - start)}s] waiting for {endpoint_name} to exist: {last}")
        time.sleep(poll_s)
    msg = f"{endpoint_name} is still building after {timeout_s}s (last status: {last}). Wait a few minutes and re-run this cell."
    if strict:
        raise TimeoutError(msg)
    print(msg)
    return False


def _load_agent_core():
    import os
    import sys

    os.environ["FQ_SCHEMA"] = FQ_SCHEMA
    os.environ["WAREHOUSE_ID"] = pick_warehouse_id()
    os.environ["MODEL_SERVICE"] = MODEL_SERVICE_FQN
    os.environ["UC_FUNCTIONS"] = UC_AGENT_FUNCTIONS
    agent_dir = f"{lab_dir()}/agent"
    if agent_dir not in sys.path:
        sys.path.insert(0, agent_dir)
    import agent_core
    import importlib

    return importlib.reload(agent_core)


def ensure_model_service():
    """Create the AI Gateway model service the agent and the approver summary both use (reused if it exists)."""
    def destination(name, model):
        return {
            "name": name,
            "destination_type": "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL",
            "pay_per_token_config": {"model": f"models/{model}"},
        }

    try:
        api("GET", f"/api/2.1/unity-catalog/model-services/{MODEL_SERVICE_FQN}")
        print(f"Reusing model service {MODEL_SERVICE_FQN}")
        return MODEL_SERVICE_FQN
    except Exception:
        pass
    api_when_ready(
        "POST",
        "/api/2.1/unity-catalog/model-services",
        query={"parent": f"schemas/{FQ_SCHEMA}", "model_service_id": MODEL_SERVICE_ID},
        body={
            "comment": f"MAPI agent and approver-summary LLM for {USER_EMAIL}",
            "config": {
                "routing": {
                    "destinations": [{**destination("primary", SUMMARY_MODEL), "traffic_percentage": 100}],
                    "fallback": {"destinations": [destination("backup", FALLBACK_MODEL)]},
                },
                "rate_limits": [
                    {"key": "RATE_LIMIT_KEY_USER_DEFAULT", "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE", "requests": 60}
                ],
                "inference_table": {"parent": f"schemas/{FQ_SCHEMA}", "table_name_prefix": "gateway"},
            },
        },
    )
    print(f"Created model service {MODEL_SERVICE_FQN}")
    return MODEL_SERVICE_FQN


def wait_for_agents_ready(timeout_s=900):
    """Wait until the model service answers and the Agent Bricks agent app is deployed and RUNNING."""
    print("Making sure your model service (the agent's LLM route) is ready...")
    api_when_ready("GET", f"/api/2.1/unity-catalog/model-services/{MODEL_SERVICE_FQN}", timeout_s=timeout_s)
    print(f"Making sure your Agent Bricks agent app {AGENT_APP_NAME} is RUNNING...")
    wait_for_deployment(AGENT_APP_NAME, timeout_s=timeout_s)
    return MODEL_SERVICE_FQN


def ask_agent(question, timeout_s=180, poll_s=10):
    """Run the agent code your Agent Bricks agent app serves. Returns (answer_text, raw_result).

    Databricks Apps only accept OAuth tokens and a notebook token is not one, so the notebook
    cannot POST to the agent app. It imports the same agent/agent_core.py the agent app runs,
    with the same tools and the same AI Gateway model service. The MAPI app calls the agent app itself.
    """
    agent_core = _load_agent_core()
    result = retry_until_ready(
        lambda: agent_core.run_agent(question),
        what="custom agent",
        timeout_s=timeout_s,
        poll_s=poll_s,
    )
    return result.get("answer") or "", result


def notebook_token():
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


def notebook_dir():
    path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    return "/Workspace" + path.rsplit("/", 1)[0]


def lab_dir():
    """The lab folder that holds app/ and agent/. Notebooks in a subfolder (such as lakebase/) get its parent."""
    import os

    d = notebook_dir()
    return d if os.path.isdir(f"{d}/app") else d.rsplit("/", 1)[0]


# Deployment copies live inside this lab folder, so the whole lab stays in one place.
APP_SOURCE_PATH = f"{lab_dir()}/app_build/{PREFIX}"
AGENT_SOURCE_PATH = f"{lab_dir()}/app_build/{PREFIX}_agent"


def register(resource_type, resource_id, display_name=""):
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} "
        "(resource_type STRING, resource_id STRING, display_name STRING, created_at TIMESTAMP)"
    )
    spark.sql(
        f"DELETE FROM {REGISTRY_TABLE} WHERE resource_type = :t AND resource_id = :i",
        args={"t": resource_type, "i": resource_id},
    )
    spark.sql(
        f"INSERT INTO {REGISTRY_TABLE} VALUES (:t, :i, :n, current_timestamp())",
        args={"t": resource_type, "i": resource_id, "n": display_name},
    )


def registered(resource_type):
    if not spark.catalog.tableExists(REGISTRY_TABLE):
        return []
    rows = spark.sql(
        f"SELECT resource_id FROM {REGISTRY_TABLE} WHERE resource_type = :t", args={"t": resource_type}
    ).collect()
    return [r.resource_id for r in rows]


def list_items(list_path, collection_key, query=None):
    items, page_token = [], None
    while True:
        q = dict(query or {})
        if page_token:
            q["page_token"] = page_token
        resp = api("GET", list_path, query=q or None)
        if isinstance(resp, list):
            return items + resp
        batch = resp.get(collection_key)
        if batch is None:
            batch = next((v for v in resp.values() if isinstance(v, list)), [])
        items += batch
        page_token = resp.get("next_page_token")
        if not page_token:
            return items


def safe_list_items(list_path, collection_key, query=None):
    try:
        return list_items(list_path, collection_key, query)
    except Exception as e:
        print(f"Could not list {list_path}: {str(e)[:120]}")
        return []


def find_by_display_name(list_path, collection_key, display_name):
    return next((i for i in list_items(list_path, collection_key) if i.get("display_name") == display_name), None)


def pick_warehouse_id():
    if LAB_WAREHOUSE_ID:
        return LAB_WAREHOUSE_ID
    whs = list(w.warehouses.list())
    preferred = [x for x in whs if x.enable_serverless_compute and "starter" in (x.name or "").lower()]
    serverless = [x for x in whs if x.enable_serverless_compute]
    chosen = (preferred or serverless or whs or [None])[0]
    assert chosen, "No SQL warehouse visible to you. Ask the presenter for a warehouse ID and set LAB_WAREHOUSE_ID."
    return chosen.id


# COMMAND ----------

# ---- Databricks Apps helpers ----


def get_app(name=None):
    try:
        return api("GET", f"/api/2.0/apps/{name or APP_NAME}")
    except Exception:
        return None


def get_agent_app():
    return get_app(AGENT_APP_NAME)


def ensure_app(name, description):
    """Create the app (its compute starts on create) unless it already exists."""
    if not get_app(name):
        api("POST", "/api/2.0/apps", body={"name": name, "description": description})
    register("app", name)


def wait_for_app_compute(name=None, timeout_s=900, poll_s=15):
    name = name or APP_NAME
    start = time.time()
    while time.time() - start < timeout_s:
        app = get_app(name) or {}
        state = (app.get("compute_status") or {}).get("state")
        if state == "ACTIVE":
            print(f"[{int(time.time() - start)}s] {name} compute ACTIVE")
            return app
        print(f"[{int(time.time() - start)}s] {name} compute: {state}")
        if state == "STOPPED":
            api("POST", f"/api/2.0/apps/{name}/start")
        time.sleep(poll_s)
    raise TimeoutError(f"{name} compute did not become ACTIVE in time - re-run this cell.")


# Streamlit theme for the MAPI app. Light only: Streamlit has no runtime theme switch, so there is no toggle.
APP_THEME_TOML = """[theme]
base = "light"
primaryColor = "#00805F"
backgroundColor = "#F7F7F7"
secondaryBackgroundColor = "#FFFFFF"
textColor = "#1C2B33"
font = "sans-serif"

[theme.sidebar]
backgroundColor = "#003753"
textColor = "#FFFFFF"
primaryColor = "#00A77F"
"""


def write_app_source(env):
    """Copy the app code from this lab folder into your own app folder and write app.yaml with your settings."""
    src_dir = f"{lab_dir()}/app"
    os_dir = APP_SOURCE_PATH
    import os

    os.makedirs(os_dir, exist_ok=True)
    for fname in ("app.py", "requirements.txt", "package.json"):
        with open(f"{src_dir}/{fname}") as f:
            content = f.read()
        with open(f"{os_dir}/{fname}", "w") as f:
            f.write(content)
    # Light theme. Streamlit cannot switch themes at runtime, so the app ships in light mode only.
    os.makedirs(f"{os_dir}/.streamlit", exist_ok=True)
    with open(f"{os_dir}/.streamlit/config.toml", "w") as f:
        f.write(APP_THEME_TOML)
    # Block-style YAML: Apps runs the command without a shell. For PGlite, npm start runs
    # pglite-server, which starts Streamlit with DATABASE_URL pointing at the in-memory Postgres.
    if env.get("BACKEND") == "pglite":
        lines = ["command:", "  - npm", "  - run", "  - start", "env:"]
    else:
        lines = ["command:", "  - streamlit", "  - run", "  - app.py", "env:"]
    for k, v in env.items():
        lines.append(f"  - name: {k}")
        lines.append(f"    value: {json.dumps(str(v))}")
    with open(f"{os_dir}/app.yaml", "w") as f:
        f.write("\n".join(lines) + "\n")
    return os_dir


def write_agent_source(env):
    """Copy the Agent Bricks agent (DurableAgentServer + agent_core) into your agent folder with your settings."""
    import os

    src_dir = f"{lab_dir()}/agent"
    os.makedirs(AGENT_SOURCE_PATH, exist_ok=True)
    for fname in ("app.py", "agent_core.py", "requirements.txt"):
        with open(f"{src_dir}/{fname}") as f:
            content = f.read()
        with open(f"{AGENT_SOURCE_PATH}/{fname}", "w") as f:
            f.write(content)
    lines = ["command:", "  - python", "  - app.py", "env:"]
    for k, v in env.items():
        lines.append(f"  - name: {k}")
        lines.append(f"    value: {json.dumps(str(v))}")
    with open(f"{AGENT_SOURCE_PATH}/app.yaml", "w") as f:
        f.write("\n".join(lines) + "\n")
    return AGENT_SOURCE_PATH


def wait_for_workspace_files(path, timeout_s=120, poll_s=3):
    """Files written through /Workspace can take a few seconds to reach the workspace file service. A deployment
    started before then snapshots only some of them (for example without app.yaml), so wait until all are listed."""
    import os

    expected = {f for f in os.listdir(path) if os.path.isfile(f"{path}/{f}")}
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            listed = {o.path.rsplit("/", 1)[-1] for o in w.workspace.list(path.removeprefix("/Workspace"))}
        except Exception:
            listed = set()
        if expected <= listed:
            return
        time.sleep(poll_s)
    print(f"Note: {path} still lists {sorted(listed)}; deploying anyway.")


def _start_deployment(name, path, env_vars=None):
    """Deploy a snapshot of path. env_vars (a dict), when given, is sent with the deployment request and replaces the env list
    in app.yaml for this deployment (the command still comes from app.yaml), so a feature flag can be flipped through the API
    without touching the source. Pass the complete environment, not just the changed variable."""
    wait_for_workspace_files(path)
    body = {"source_code_path": path, "mode": "SNAPSHOT"}
    if env_vars:
        body["env_vars"] = [{"name": k, "value": str(v)} for k, v in env_vars.items()]
    # The workspace folder can take a moment to appear after the copy, so also retry "Path does not exist".
    dep = retry_until_ready(
        lambda: api("POST", f"/api/2.0/apps/{name}/deployments", body=body),
        what=f"{name} deployment slot",
        timeout_s=600,
        poll_s=15,
        retry_if=lambda e: "does not exist" in str(e).lower() or _still_provisioning(e),
    )
    print(f"{name}: deployment started {dep.get('deployment_id')} (takes 1-3 min)")
    return dep


def deploy_app(env, env_vars=None):
    return _start_deployment(APP_NAME, write_app_source(env), env_vars)


def deploy_agent_app():
    return _start_deployment(AGENT_APP_NAME, write_agent_source(agent_env()))


def wait_for_deployment(name=None, timeout_s=900, poll_s=15):
    name = name or APP_NAME
    start = time.time()
    while time.time() - start < timeout_s:
        app = get_app(name) or {}
        dep = app.get("pending_deployment") or app.get("active_deployment") or {}
        state = (dep.get("status") or {}).get("state")
        app_state = (app.get("app_status") or {}).get("state")
        if not app.get("pending_deployment") and not app.get("active_deployment") and time.time() - start > 90:
            raise RuntimeError(f"{name} has not been deployed yet. Run the last cell of 02_Agent_Tools (2.4 Deploy your apps), then re-run this cell.")
        print(f"[{int(time.time() - start)}s] {name} deployment: {state} | app: {app_state}")
        if state == "SUCCEEDED" and app_state == "RUNNING" and not app.get("pending_deployment"):
            return app
        if state == "FAILED":
            raise RuntimeError((dep.get("status") or {}).get("message"))
        time.sleep(poll_s)
    raise TimeoutError(f"{name} deployment still running - re-run this cell.")


def app_resources(include_model_service=True, include_lakebase=False):
    res = [
        {"name": "sql-warehouse", "sql_warehouse": {"id": pick_warehouse_id(), "permission": "CAN_USE"}},
    ]
    if include_model_service:
        res.append({"name": "llm", "uc_securable": {"securable_full_name": MODEL_SERVICE_FQN, "securable_type": "MODEL_SERVICE", "permission": "EXECUTE"}})
    if include_lakebase:
        res.append({"name": "lakebase", "postgres": {"branch": f"{LAKEBASE_PROJECT_PATH}/branches/production", "database": lakebase_database_name(), "permission": "CAN_CONNECT_AND_CREATE"}})
    return res


def set_app_resources(resources, name=None):
    name = name or APP_NAME
    app = get_app(name) or {}
    retry_until_ready(
        lambda: api("PATCH", f"/api/2.0/apps/{name}", body={"name": name, "description": app.get("description", ""), "resources": resources}),
        what=f"{name} resources",
        timeout_s=600,
        poll_s=15,
    )
    print(f"{name} resources: " + ", ".join(r["name"] for r in resources))


def agent_app_url():
    return (get_agent_app() or {}).get("url")


def agent_env():
    return {
        "FQ_SCHEMA": FQ_SCHEMA,
        "WAREHOUSE_ID": pick_warehouse_id(),
        "UC_FUNCTIONS": UC_AGENT_FUNCTIONS,
        "MODEL_SERVICE": MODEL_SERVICE_FQN,
    }


def app_env(backend="uc"):
    agent_url = agent_app_url()
    assert agent_url, f"{AGENT_APP_NAME} has no URL yet - run 01_Setup_and_Kickoff first."
    env = {
        "BACKEND": backend,
        "FQ_SCHEMA": FQ_SCHEMA,
        "WAREHOUSE_ID": pick_warehouse_id(),
        "UC_FUNCTIONS": UC_AGENT_FUNCTIONS,
        "MODEL_SERVICE": MODEL_SERVICE_FQN,
        "AGENT_URL": agent_url,
    }
    if backend == "lakebase":
        ep = lakebase_endpoint()
        env.update({"LAKEBASE_ENDPOINT": ep["name"], "LAKEBASE_HOST": ep["status"]["hosts"]["host"], "LAKEBASE_DB": LAKEBASE_DB, "LAKEBASE_SCHEMA": LAKEBASE_PG_SCHEMA})
    return env


def app_sp_client_id():
    return (get_app() or {}).get("service_principal_client_id")


def wait_for_app_sp(name=None, timeout_s=300, poll_s=10):
    name = name or APP_NAME
    start = time.time()
    sp = (get_app(name) or {}).get("service_principal_client_id")
    while not sp:
        if time.time() - start > timeout_s:
            raise TimeoutError(f"{name} has no service principal yet - wait a minute and re-run this cell.")
        print(f"[{int(time.time() - start)}s] waiting for {name}'s service principal...")
        time.sleep(poll_s)
        sp = (get_app(name) or {}).get("service_principal_client_id")
    return sp


def grant_schema_to_app(name=None, privileges="USE SCHEMA, SELECT, MODIFY, EXECUTE"):
    sp = wait_for_app_sp(name)
    for stmt in [
        f"GRANT USE CATALOG ON CATALOG {LAB_CATALOG} TO `{sp}`",
        f"GRANT {privileges} ON SCHEMA {FQ_SCHEMA} TO `{sp}`",
    ]:
        try:
            spark.sql(stmt)
        except Exception as e:
            print(f"Note: {stmt.split(' TO ')[0]} -> {str(e)[:150]}")
    return sp


def grant_agent_to_app():
    """Let the MAPI app's service principal call the agent app (CAN_USE on the agent app)."""
    sp = wait_for_app_sp(APP_NAME)
    retry_until_ready(
        lambda: api(
            "PATCH",
            f"/api/2.0/permissions/apps/{AGENT_APP_NAME}",
            body={"access_control_list": [{"service_principal_name": sp, "permission_level": "CAN_USE"}]},
        ),
        what="agent app permissions",
        timeout_s=300,
        poll_s=15,
    )
    return sp


# COMMAND ----------

# ---- Lakebase helpers ----

LAKEBASE_PROJECT_PATH = f"projects/{LAKEBASE_PROJECT}"


def lakebase_endpoint(branch="production"):
    eps = list_items(f"/api/2.0/postgres/{LAKEBASE_PROJECT_PATH}/branches/{branch}/endpoints", "endpoints")
    rw = [e for e in eps if (e.get("status") or {}).get("endpoint_type", e.get("spec", {}).get("endpoint_type")) != "ENDPOINT_TYPE_READ_ONLY"]
    return (rw or eps or [None])[0]


def lakebase_database_name(branch="production"):
    dbs = safe_list_items(f"/api/2.0/postgres/{LAKEBASE_PROJECT_PATH}/branches/{branch}/databases", "databases")
    match = [d for d in dbs if (d.get("status") or d.get("spec") or {}).get("postgres_database") == LAKEBASE_DB or d.get("name", "").endswith(f"/{LAKEBASE_DB}")]
    return match[0]["name"] if match else f"{LAKEBASE_PROJECT_PATH}/branches/{branch}/databases/{LAKEBASE_DB}"


def lakebase_connect(branch="production", database=None):
    import psycopg

    ep = lakebase_endpoint(branch)
    assert ep, f"No endpoint on branch {branch} yet."
    host = ep["status"]["hosts"]["host"]
    token = api("POST", "/api/2.0/postgres/credentials", body={"endpoint": ep["name"]})["token"]
    return psycopg.connect(
        host=host,
        port=5432,
        dbname=database or LAKEBASE_DB,
        user=USER_EMAIL,
        password=token,
        sslmode="require",
        autocommit=True,
    )


def wait_for_lakebase(branch="production", timeout_s=600, poll_s=15):
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            ep = lakebase_endpoint(branch)
            state = (ep or {}).get("status", {}).get("current_state")
            if ep and state in ("ACTIVE", "IDLE"):
                print(f"[{int(time.time() - start)}s] Lakebase {branch} endpoint is {state}")
                return ep
            print(f"[{int(time.time() - start)}s] Lakebase {branch} endpoint: {state}")
        except Exception as e:
            print(f"[{int(time.time() - start)}s] waiting for Lakebase: {str(e)[:120]}")
        time.sleep(poll_s)
    raise TimeoutError("Lakebase not ready yet - re-run this cell.")


def show_link(label, url):
    """Print a clickable link in the notebook (and a plain URL for job/log output)."""
    if not url:
        print(f"{label}: (not ready yet)")
        return
    print(f"{label}: {url}")
    try:
        displayHTML(f'<div style="margin:4px 0"><b>{label}:</b> <a href="{url}" target="_blank">{url}</a></div>')
    except NameError:
        pass


def schema_url():
    return f"{WORKSPACE_HOST}/explore/data/{LAB_CATALOG}/{SCHEMA}"


def endpoint_url(endpoint_name):
    return f"{WORKSPACE_HOST}/ml/endpoints/{endpoint_name}" if endpoint_name else None


def model_service_url():
    # Catalog Explorer page for the model-service securable, plus the AI Gateway Models list.
    return f"{WORKSPACE_HOST}/explore/data/{LAB_CATALOG}/{SCHEMA}/model_services/{MODEL_SERVICE_ID}"


def ai_gateway_models_url():
    return f"{WORKSPACE_HOST}/ai-gateway/models"


def app_manage_url():
    return f"{WORKSPACE_HOST}/apps/{APP_NAME}"


def app_open_url():
    return (get_app() or {}).get("url")


def agent_app_manage_url():
    return f"{WORKSPACE_HOST}/apps/{AGENT_APP_NAME}"


def experiment_url(experiment_id):
    return f"{WORKSPACE_HOST}/ml/experiments/{experiment_id}" if experiment_id else None


def guardrail_url():
    return f"{WORKSPACE_HOST}/explore/data/{LAB_CATALOG}/mapi_policies/functions/block_ai_ad_copy_{USER_SLUG}"


print(f"Hello {USER_EMAIL}")
print(f"  Your lab prefix : {PREFIX}")
print(f"  Your schema     : {FQ_SCHEMA}")
print(f"  Your MAPI app   : {APP_NAME}")
print(f"  Your agent app  : {AGENT_APP_NAME}")
show_link("Your schema", schema_url())
