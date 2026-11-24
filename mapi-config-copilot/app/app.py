"""MAPI config editor + assistant (showcase lab).

BACKEND=pglite   -> Agent Bricks session. PGlite (Postgres compiled to WebAssembly) runs in memory next to
                    Streamlit: app.yaml starts pglite-server, which launches Streamlit with DATABASE_URL set.
                    Rows are copied in from Unity Catalog on first use. Edits, approvals and the change log
                    reset when the app restarts or is redeployed. The SQL is plain Postgres, so moving to
                    Lakebase only changes the connection.
BACKEND=uc       -> rows read and written in Unity Catalog tables (kept for reference)
BACKEND=lakebase -> rows live in Lakebase Postgres (Lakebase session)

Feature flag USE_LAKEBASE=true (set by lakebase/02_redeploy_app_with_lakebase) switches BACKEND to lakebase,
whatever BACKEND says. The app finds the read-write endpoint of LAKEBASE_PROJECT / LAKEBASE_BRANCH and logs in
as its own service principal with short-lived OAuth tokens. Without the flag nothing changes.

The Metrics tab reads METRICS_TABLE (a Lakebase synced table created by lakebase/03_metrics_and_sync).
Until that table exists in the database the tab says "Coming soon...".
"""

import hashlib
import json
import os
import re
import threading
import time
import uuid

import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config


def _flag(name):
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "on")


USE_LAKEBASE = _flag("USE_LAKEBASE")
BACKEND = "lakebase" if USE_LAKEBASE else os.getenv("BACKEND", "pglite")
FQ_SCHEMA = os.getenv("FQ_SCHEMA", "")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "")
UC_FUNCTIONS = os.getenv("UC_FUNCTIONS", "search_rows,propose_change_set,find_issues,lookup_rules")
MODEL_SERVICE = os.getenv("MODEL_SERVICE", "")
AGENT_URL = os.getenv("AGENT_URL", "").rstrip("/")
DATABASE_URL = os.getenv("DATABASE_URL", "")
LAKEBASE_PROJECT = os.getenv("LAKEBASE_PROJECT", "")
LAKEBASE_BRANCH = os.getenv("LAKEBASE_BRANCH", "production")
# Fallback when the project lookup is not possible: set by the redeploy notebook (the platform also injects
# LAKEBASE_ENDPOINT / PGHOST / PGDATABASE for an app with a Lakebase resource).
LAKEBASE_ENDPOINT = os.getenv("LAKEBASE_ENDPOINT", "")
LAKEBASE_HOST = os.getenv("LAKEBASE_HOST") or os.getenv("PGHOST", "")
LAKEBASE_DB = os.getenv("LAKEBASE_DB") or os.getenv("PGDATABASE") or "databricks_postgres"
PG_SCHEMA = os.getenv("PG_SCHEMA") or os.getenv("LAKEBASE_SCHEMA", "mapi")
METRICS_TABLE = os.getenv("METRICS_TABLE", "")  # <schema>.<table> of the synced metrics table

cfg = Config()
w = WorkspaceClient()

st.set_page_config(page_title="MAPI config", layout="wide")

# Light theme, set in .streamlit/config.toml (written by the setup notebook). Colours and fonts are
# inspired by the brand palette. Nothing here is a real logo: the mark below is made up for the demo.
st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800&display=swap');
html, body, .stApp, [data-testid="stMarkdownContainer"], button, input, textarea { font-family: 'Nunito', sans-serif; }
h1, h2, h3 { color: #003753; }
.mapi-header { display: flex; align-items: center; gap: 14px; margin: 0.25rem 0 0.5rem 0; }
.mapi-mark {
  width: 46px; height: 46px; border-radius: 12px; background: #00805F; color: #FFFFFF;
  display: flex; align-items: center; justify-content: center; font-weight: 800; font-size: 22px;
  box-shadow: inset 0 -4px 0 #003753;
}
.mapi-title { font-size: 26px; font-weight: 800; color: #003753; line-height: 1.1; }
.mapi-sub { color: #5E5E5E; font-size: 14px; }
</style>
""",
    unsafe_allow_html=True,
)

# Chip colours for the brand and status columns. Text stays in the cell; colour is an extra cue.
BRAND_COLOURS = {
    "Real Funeral": ("#912680", "#FFFFFF"),
    "Real Insurance": ("#912680", "#FFFFFF"),
    "OneChoice": ("#006F32", "#FFFFFF"),
    "My Comparison Life": ("#003753", "#FFFFFF"),
}
STATUS_COLOURS = {
    "draft": ("#FFF4D6", "#7A5A00"),
    "approved": ("#E3F4EE", "#00805F"),
    "archived": ("#E9ECEF", "#5E5E5E"),
}


def style_table(df, brand_col=None, status_col=None):
    """Colour the brand and status cells. Returns a pandas Styler for st.dataframe."""
    styler = df.style
    if brand_col and brand_col in df.columns:
        styler = styler.map(
            lambda v: f"background-color: {BRAND_COLOURS[v][0]}; color: {BRAND_COLOURS[v][1]}; font-weight: 700"
            if v in BRAND_COLOURS else "",
            subset=[brand_col],
        )
    if status_col and status_col in df.columns:
        styler = styler.map(
            lambda v: f"background-color: {STATUS_COLOURS[v][0]}; color: {STATUS_COLOURS[v][1]}; font-weight: 700"
            if v in STATUS_COLOURS else "",
            subset=[status_col],
        )
    return styler


def table(name):
    if BACKEND in ("pglite", "lakebase"):
        return f"{PG_SCHEMA}.{name}"
    return f"{FQ_SCHEMA}.{name}"


# Postgres tables, in the same shape as the Unity Catalog seed tables.
PG_TABLES = {
    "mapi_rows": {
        "row_id": "TEXT PRIMARY KEY", "brand": "TEXT", "product": "TEXT", "campaign": "TEXT", "text_asset_id": "TEXT",
        "primary_copy": "TEXT", "landing_url": "TEXT", "status": "TEXT", "approved_hash": "TEXT", "approved_by": "TEXT",
        "approved_at": "TIMESTAMPTZ", "last_edited_by": "TEXT", "updated_at": "TIMESTAMPTZ",
        "row_version": "INT NOT NULL DEFAULT 1", "last_uploaded_at": "TIMESTAMPTZ",
    },
    "change_log": {
        "change_id": "TEXT PRIMARY KEY", "row_id": "TEXT", "field": "TEXT", "old_value": "TEXT", "new_value": "TEXT",
        "changed_by": "TEXT", "changed_at": "TIMESTAMPTZ", "reason": "TEXT", "source": "TEXT",
    },
    "upload_controls": {"control": "TEXT PRIMARY KEY", "value": "TEXT", "updated_by": "TEXT", "updated_at": "TIMESTAMPTZ"},
}


def content_hash(r):
    return hashlib.sha256(
        "|".join([r["brand"], r["product"], r["primary_copy"], r["landing_url"], r["text_asset_id"]]).encode()
    ).hexdigest()


# ---------------- data access ----------------


def _uc_conn():
    from databricks import sql

    return sql.connect(
        server_hostname=cfg.host.replace("https://", ""),
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
    )


@st.cache_resource
def lakebase_target():
    """(endpoint path, host) of the read-write compute of LAKEBASE_PROJECT / LAKEBASE_BRANCH."""
    if LAKEBASE_PROJECT:
        try:
            path = f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}/endpoints"
            for ep in w.api_client.do("GET", path).get("endpoints") or []:
                kind = (ep.get("status") or {}).get("endpoint_type") or (ep.get("spec") or {}).get("endpoint_type")
                host = ((ep.get("status") or {}).get("hosts") or {}).get("host")
                if kind == "ENDPOINT_TYPE_READ_WRITE" and host:
                    print(f"Lakebase: {ep['name']} at {host}, database {LAKEBASE_DB}")
                    return ep["name"], host
        except Exception as e:
            print(f"Lakebase: could not look up {LAKEBASE_PROJECT}/{LAKEBASE_BRANCH} ({e}); using LAKEBASE_ENDPOINT")
    if LAKEBASE_ENDPOINT and LAKEBASE_HOST:
        return LAKEBASE_ENDPOINT, LAKEBASE_HOST
    raise RuntimeError(
        f"USE_LAKEBASE is on, but no Lakebase endpoint was found for project '{LAKEBASE_PROJECT}' branch "
        f"'{LAKEBASE_BRANCH}'. Re-run lakebase/02_redeploy_app_with_lakebase."
    )


@st.cache_resource
def _token_cache():
    return {"token": None, "expires": 0.0, "lock": threading.Lock()}


def _lakebase_token():
    """OAuth token used as the Postgres password. Tokens last an hour; refresh well before that."""
    cache = _token_cache()
    with cache["lock"]:
        if time.time() > cache["expires"]:
            endpoint, _ = lakebase_target()
            cache["token"] = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint})["token"]
            cache["expires"] = time.time() + 40 * 60
        return cache["token"]


@st.cache_resource
def lakebase_pool():
    """Connection pool shared by every user of the app. New connections log in with a current OAuth token,
    as the app's service principal (DATABRICKS_CLIENT_ID), or as you when the app runs locally."""
    import psycopg
    from psycopg_pool import ConnectionPool

    _, host = lakebase_target()
    pg_user = os.getenv("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name

    class OAuthConnection(psycopg.Connection):
        @classmethod
        def connect(cls, conninfo="", **kwargs):
            kwargs["password"] = _lakebase_token()
            return super().connect(conninfo, **kwargs)

    return ConnectionPool(
        connection_class=OAuthConnection,
        kwargs={"host": host, "port": 5432, "dbname": LAKEBASE_DB, "user": pg_user, "sslmode": "require", "autocommit": True},
        min_size=1,
        max_size=5,
        max_lifetime=45 * 60,
        check=ConnectionPool.check_connection,  # drops connections closed while the compute scaled to zero
        open=True,
    )


@st.cache_resource
def pglite_db():
    """One PGlite connection per app process, shared by all users (PGlite serves one connection at a time).

    Creates the tables and seeds them from Unity Catalog the first time the app loads. PGlite keeps
    everything in memory, so a restart or redeploy starts again from the seed data.
    """
    import psycopg

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set. Start the app with pglite-server (see app.yaml).")
    conn = psycopg.connect(DATABASE_URL, sslmode="disable", gssencmode="disable", autocommit=True)
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA}")
    for name, columns in PG_TABLES.items():
        conn.execute(f"CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.{name} ({', '.join(f'{c} {t}' for c, t in columns.items())})")
    if conn.execute(f"SELECT count(*) FROM {PG_SCHEMA}.mapi_rows").fetchone()[0] == 0:
        with _uc_conn() as uc, uc.cursor() as cur:
            for name, columns in PG_TABLES.items():
                cols = ", ".join(columns)
                cur.execute(f"SELECT {cols} FROM {FQ_SCHEMA}.{name}")
                rows = [tuple(r) for r in cur.fetchall()]
                if rows:
                    with conn.cursor() as pg:
                        pg.executemany(
                            f"INSERT INTO {PG_SCHEMA}.{name} ({cols}) VALUES ({', '.join(['%s'] * len(columns))})", rows
                        )
    return conn, threading.Lock()


def _pg_execute(conn, sql_text, params, fetch):
    sql_text = re.sub(r"(?<!:):(\w+)", r"%(\1)s", sql_text)
    with conn.cursor() as cur:
        cur.execute(sql_text, params)
        if fetch and cur.description:
            return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
        return cur.rowcount


def run(sql_text, params=None, fetch=True):
    params = params or {}
    if BACKEND == "pglite":
        import psycopg

        try:
            conn, lock = pglite_db()
            with lock:
                return _pg_execute(conn, sql_text, params, fetch)
        except psycopg.OperationalError:
            pglite_db.clear()
            conn, lock = pglite_db()
            with lock:
                return _pg_execute(conn, sql_text, params, fetch)
    if BACKEND == "lakebase":
        import psycopg

        try:
            with lakebase_pool().connection() as conn:
                return _pg_execute(conn, sql_text, params, fetch)
        except psycopg.OperationalError:
            # The compute may have just woken from scale-to-zero. One retry on a fresh connection.
            time.sleep(1)
            with lakebase_pool().connection() as conn:
                return _pg_execute(conn, sql_text, params, fetch)
    with _uc_conn() as conn, conn.cursor() as cur:
        cur.execute(sql_text, params)
        if fetch and cur.description:
            return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
        return None


def load_rows():
    return run(
        f"SELECT row_id, brand, product, campaign, text_asset_id, status, landing_url, primary_copy, "
        f"last_edited_by, approved_by, approved_hash, row_version FROM {table('mapi_rows')} ORDER BY row_id"
    )


def get_row(row_id):
    df = run(f"SELECT * FROM {table('mapi_rows')} WHERE row_id = :id", {"id": row_id})
    return None if df is None or df.empty else df.iloc[0].to_dict()


def log_change(row_id, field, old, new, user, reason, source):
    run(
        f"INSERT INTO {table('change_log')} (change_id, row_id, field, old_value, new_value, changed_by, changed_at, reason, source) "
        "VALUES (:cid, :rid, :f, :o, :n, :u, now(), :r, :s)",
        {"cid": str(uuid.uuid4()), "rid": row_id, "f": field, "o": str(old), "n": str(new), "u": user, "r": reason, "s": source},
        fetch=False,
    )


EDITABLE = {"landing_url", "campaign", "status", "primary_copy"}


def save_edit(row_id, field, new_value, expected_version, user, reason, source="editor"):
    """Optimistic save: rejected if someone else saved the row first."""
    assert field in EDITABLE
    current = get_row(row_id)
    if current is None:
        return "Row not found."
    if int(current["row_version"]) != int(expected_version):
        return "Row changed since you loaded it. Reload and try again."
    if BACKEND == "lakebase":
        # The database trigger resets approval and bumps row_version.
        n = run(
            f"UPDATE {table('mapi_rows')} SET {field} = :v, last_edited_by = :u WHERE row_id = :id AND row_version = :ver",
            {"v": new_value, "u": user, "id": row_id, "ver": int(expected_version)},
            fetch=False,
        )
        if n == 0:
            return "Row changed since you loaded it. Reload and try again."
    else:
        reset = (
            ", status = CASE WHEN status = 'approved' THEN 'draft' ELSE status END, approved_hash = NULL, approved_by = NULL, approved_at = NULL"
            if field != "status"
            else ""
        )
        n = run(
            f"UPDATE {table('mapi_rows')} SET {field} = :v, last_edited_by = :u, updated_at = now(), row_version = row_version + 1{reset} "
            "WHERE row_id = :id AND row_version = :ver",
            {"v": new_value, "u": user, "id": row_id, "ver": int(expected_version)},
            fetch=False,
        )
        if n == 0:
            return "Row changed since you loaded it. Reload and try again."
    log_change(row_id, field, current[field], new_value, user, reason, source)
    return None


def approve(row_id, user):
    row = get_row(row_id)
    if row["status"] != "draft":
        return "Only drafts can be approved."
    if row["last_edited_by"] == user:
        return "You edited this row last, so someone else must approve it."
    try:
        run(
            f"UPDATE {table('mapi_rows')} SET status = 'approved', approved_hash = :h, approved_by = :u, approved_at = now() WHERE row_id = :id",
            {"h": content_hash(row), "u": user, "id": row_id},
            fetch=False,
        )
    except Exception as e:
        return f"Database rejected the approval: {str(e)[:200]}"
    log_change(row_id, "status", "draft", "approved", user, "approved in approval queue", "approver")
    return None


def uploads_paused():
    df = run(f"SELECT value FROM {table('upload_controls')} WHERE control = 'uploads_paused'")
    return df is not None and not df.empty and str(df.iloc[0]["value"]).lower() == "true"


def set_uploads_paused(value, user):
    run(
        f"UPDATE {table('upload_controls')} SET value = :v, updated_by = :u, updated_at = now() WHERE control = 'uploads_paused'",
        {"v": "true" if value else "false", "u": user},
        fetch=False,
    )


def load_metrics():
    """The synced metrics table, or None while it is not in this database (or not readable by the app yet)."""
    if "." not in METRICS_TABLE:
        return None
    schema, name = METRICS_TABLE.split(".", 1)
    try:
        found = run(
            "SELECT count(*) AS n FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace s ON s.oid = c.relnamespace "
            "WHERE s.nspname = :s AND c.relname = :t",
            {"s": schema, "t": name},
        )
        if found is None or int(found.iloc[0]["n"]) == 0:
            return None
        return run(f"SELECT * FROM {METRICS_TABLE}")
    except Exception as e:
        print(f"Metrics table {METRICS_TABLE} not readable yet: {str(e)[:200]}")
        return None


# Approved / draft / archived fills (validated against the white chart surface; archived is a deliberate neutral).
STATUS_FILLS = ["#00805F", "#EDA100", "#898781"]
SERIES_FILLS = ["#2A78D6", "#EB6834", "#1BAF7A"]


def render_metrics(m):
    m = m.copy()
    numeric = [c for c in m.columns if c not in ("brand", "campaign", "campaign_month", "last_activity_at", "refreshed_at")]
    m[numeric] = m[numeric].apply(pd.to_numeric, errors="coerce")  # Postgres NUMERIC arrives as Decimal objects
    active = int(m.approved_rows.sum() + m.draft_rows.sum())
    approvals = int(m.approvals.sum())
    hours = (m.avg_hours_to_approve.fillna(0) * m.approvals).sum() / approvals if approvals else None
    edits = int(m.edits.sum())

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Active rows", f"{active:,}", help="Approved + draft rows across every campaign")
    k2.metric("Drafts waiting", f"{int(m.draft_rows.sum()):,}")
    k3.metric("Rows with issues", f"{int(m.issue_rows.sum()):,}", help="Off-brand URL, wrong-brand copy, product not sold by the brand, or approved content changed after approval")
    k4.metric("Hours to approval", f"{hours:.1f}" if hours is not None else "-", help="Average time from a row's last edit to its approval")
    k5.metric("Edits by the assistant", f"{m.assistant_edits.sum() / edits:.0%}" if edits else "-")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Rows by brand and status**")
        by_status = m.groupby("brand")[["approved_rows", "draft_rows", "archived_rows"]].sum()
        by_status.columns = ["Approved", "Draft", "Archived"]
        st.bar_chart(by_status, horizontal=True, stack=True, color=STATUS_FILLS, x_label="", y_label="Rows", height=300)
    with c2:
        st.markdown("**Hours from last edit to approval, by brand**")
        rated = m[m.approvals > 0].assign(w=lambda d: d.avg_hours_to_approve.fillna(0) * d.approvals)
        by_brand = rated.groupby("brand")[["w", "approvals"]].sum()
        by_brand["Hours"] = (by_brand.w / by_brand.approvals).round(1)
        st.bar_chart(by_brand[["Hours"]], horizontal=True, color=SERIES_FILLS[0], x_label="", y_label="Average hours", height=300)

    st.markdown("**Edits and approvals by campaign month**")
    trend = m.groupby("campaign_month")[["people_edits", "assistant_edits", "approvals"]].sum().sort_index()
    trend.index = pd.to_datetime(trend.index).strftime("%Y-%m")  # one point per campaign month
    trend.columns = ["Edits by people", "Edits by the assistant", "Approvals"]
    st.line_chart(trend, color=SERIES_FILLS, x_label="Campaign month", y_label="Changes", height=320)

    issues = m[m.issue_rows > 0][["brand", "campaign", "issue_rows", "total_rows"]].sort_values("issue_rows", ascending=False)
    st.markdown("**Where the issues are**")
    if issues.empty:
        st.info("No rows with issues.")
    else:
        st.dataframe(style_table(issues, brand_col="brand"), hide_index=True, use_container_width=True)

    refreshed = pd.to_datetime(m.refreshed_at).max() if "refreshed_at" in m else None
    st.caption(f"Lakebase synced table {METRICS_TABLE}, aggregated in Unity Catalog and synced back to Postgres"
               + (f" · refreshed {refreshed:%d %b %Y %H:%M} UTC" if refreshed is not None and not pd.isna(refreshed) else ""))
    with st.expander("Data table"):
        st.dataframe(m.sort_values(["brand", "campaign"]), hide_index=True, use_container_width=True)


# ---------------- assistant + gateway ----------------


def _agent_request(method, path, body=None):
    import urllib.error
    import urllib.request

    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json", **cfg.authenticate()}
    req = urllib.request.Request(f"{AGENT_URL}{path}", data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Agent app returned HTTP {e.code}: {e.read().decode()[:300]}") from e


def run_agent_remote(question, timeout_s=240, poll_s=2):
    """Call the Agent Bricks agent app as this app's service principal (needs CAN_USE on the agent app)."""
    if not AGENT_URL:
        raise RuntimeError("AGENT_URL is not set - redeploy the app from 02_Agent_Tools.")
    body = {"id": str(uuid.uuid4()), "background": True, "input": {"messages": [{"role": "user", "content": question}]}}
    state = _agent_request("POST", "/api/invocations", body)
    deadline = time.time() + timeout_s
    while state.get("status") not in ("completed", "failed"):
        if time.time() > deadline:
            raise TimeoutError("The agent did not answer in time - try again.")
        time.sleep(poll_s)
        state = _agent_request("GET", state.get("status_url") or f"/api/invocations/{body['id']}")
    if state["status"] == "failed":
        raise RuntimeError(f"Agent failed: {(state.get('error') or {}).get('message', state.get('error'))}")
    return state.get("output") or {}


def ask_assistant(question):
    result = run_agent_remote(question)
    answer = result.get("answer") or ""
    change_set = result.get("change_set")
    if change_set is None:
        for block in re.findall(r"```json\s*(\{.*?\})\s*```", answer, flags=re.S):
            try:
                obj = json.loads(block)
                if "proposed_changes" in obj:
                    change_set = obj
            except Exception:
                pass
    answer = re.sub(r"```json.*?```", "", answer, flags=re.S).strip()
    return answer, change_set


def summarise_for_approver(diff_text):
    from openai import OpenAI

    token = cfg.authenticate()["Authorization"].split(" ", 1)[1]
    client = OpenAI(api_key=token, base_url=f"{cfg.host.rstrip('/')}/ai-gateway/mlflow/v1")
    r = client.chat.completions.create(
        model=MODEL_SERVICE,
        messages=[
            {"role": "system", "content": "Summarise these pending MAPI ad-config changes for a Digital Performance approver in 3-5 bullet points. Flag anything that looks off-brand or risky."},
            {"role": "user", "content": diff_text},
        ],
        max_tokens=300,
    )
    return r.choices[0].message.content, r.model


# ---------------- UI ----------------

user = st.context.headers.get("X-Forwarded-Email") or "local.user@example.com"

st.markdown(
    '<div class="mapi-header"><div class="mapi-mark">M</div><div>'
    '<div class="mapi-title">MAPI text-ad config</div>'
    '<div class="mapi-sub">Meta text-ad rows: edit, ask the assistant, approve</div>'
    "</div></div>",
    unsafe_allow_html=True,
)
if BACKEND == "lakebase":
    storage_label = f"Lakebase Postgres ({LAKEBASE_PROJECT or 'project'} / {LAKEBASE_BRANCH})"
elif BACKEND == "pglite":
    storage_label = "PGlite, in-memory Postgres (resets when the app restarts)"
else:
    storage_label = "Unity Catalog tables"
st.caption(f"Signed in as {user} · storage: {storage_label}")

with st.sidebar:
    st.subheader("Uploads to Meta")
    try:
        paused = uploads_paused()
        st.write("Status: **PAUSED**" if paused else "Status: running on schedule")
        if st.button("Resume uploads" if paused else "Pause uploads"):
            set_uploads_paused(not paused, user)
            st.rerun()
    except Exception as e:
        st.error(f"Could not read upload controls: {e}")

tab_edit, tab_assist, tab_approve, tab_history, tab_metrics = st.tabs(
    ["Editor", "Assistant", "Approval queue", "Row history", "Metrics"]
)

with tab_edit:
    try:
        df = load_rows()
    except Exception as e:
        st.error(f"Could not load rows: {e}")
        st.stop()
    c1, c2, c3 = st.columns(3)
    brands = c1.multiselect("Brand (your filter only)", sorted(df.brand.unique()))
    campaigns = c2.multiselect("Campaign", sorted(df.campaign.unique()))
    statuses = c3.multiselect("Status", ["draft", "approved", "archived"])
    view = df
    if brands:
        view = view[view.brand.isin(brands)]
    if campaigns:
        view = view[view.campaign.isin(campaigns)]
    if statuses:
        view = view[view.status.isin(statuses)]
    st.dataframe(
        style_table(view.drop(columns=["approved_hash"]), brand_col="brand", status_col="status"),
        hide_index=True, use_container_width=True, height=360,
    )

    st.markdown("**Edit one row**")
    e1, e2 = st.columns([1, 2])
    row_id = e1.selectbox("Row", view.row_id.tolist() if not view.empty else [])
    field = e1.selectbox("Field", ["landing_url", "campaign", "primary_copy"])
    if row_id:
        current = df[df.row_id == row_id].iloc[0]
        new_value = e2.text_input("New value", value=str(current[field]))
        reason = e2.text_input("Reason for change (required)")
        if current["status"] == "approved":
            e2.warning("This row is approved. Saving a change sends it back to draft for re-approval.")
        if e2.button("Save draft"):
            if not reason.strip():
                st.error("A reason is required.")
            else:
                err = save_edit(row_id, field, new_value, current["row_version"], user, reason)
                if err:
                    st.error(err)
                else:
                    st.success(f"Saved {row_id}.")

with tab_assist:
    st.markdown("Ask for bulk edits, quality checks or counts. The assistant **proposes**; nothing is saved until you accept.")
    st.caption(f"Served by your Agent Bricks agent app; every LLM call goes through {MODEL_SERVICE or 'your AI Gateway model service'}.")
    st.caption('Try: "Change the landing page URL for all Real Funeral t001 rows to https://www.realfuneral.example/funeral-cover/quote"')
    question = st.chat_input("Ask the MAPI assistant")
    if question:
        st.session_state["last_q"] = question
        with st.spinner("Assistant is working..."):
            try:
                st.session_state["last_answer"] = ask_assistant(question)
            except Exception as e:
                st.session_state["last_answer"] = (f"Assistant error: {e}", None)
    if "last_answer" in st.session_state:
        answer, change_set = st.session_state["last_answer"]
        st.chat_message("user").write(st.session_state["last_q"])
        st.chat_message("assistant").write(answer)
        if change_set and change_set.get("proposed_changes") and not change_set.get("blocked_reason"):
            preview = pd.DataFrame(change_set["proposed_changes"])
            preview.insert(0, "accept", True)
            st.markdown(f"**Preview: {len(preview)} rows** (untick any you do not want)")
            edited = st.data_editor(preview, hide_index=True, use_container_width=True, disabled=[c for c in preview.columns if c != "accept"])
            if st.button("Save accepted rows as drafts"):
                current = load_rows().set_index("row_id")
                saved, errors = 0, []
                for _, ch in edited[edited.accept].iterrows():
                    err = save_edit(ch.row_id, ch.field, ch.new_value, current.loc[ch.row_id, "row_version"], user,
                                    st.session_state["last_q"], source="assistant")
                    saved += err is None
                    if err:
                        errors.append(f"{ch.row_id}: {err}")
                st.success(f"Saved {saved} rows as drafts. They are now in the approval queue.")
                for msg in errors:
                    st.warning(msg)
        elif change_set and change_set.get("blocked_reason"):
            st.info(f"Blocked: {change_set['blocked_reason']}")

with tab_approve:
    st.markdown("Drafts waiting for approval. You cannot approve a row you edited last.")
    try:
        drafts = run(
            f"SELECT r.row_id, r.brand, r.product, r.campaign, r.landing_url, r.last_edited_by, "
            f"l.old_value, l.new_value, l.field, l.reason, l.source "
            f"FROM {table('mapi_rows')} r LEFT JOIN (SELECT row_id, field, old_value, new_value, reason, source, "
            f"ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY changed_at DESC) rn FROM {table('change_log')} "
            f"WHERE field <> 'status') l ON r.row_id = l.row_id AND l.rn = 1 "
            f"WHERE r.status = 'draft' AND l.row_id IS NOT NULL ORDER BY r.row_id"
        )
    except Exception as e:
        drafts = pd.DataFrame()
        st.error(f"Could not load the queue: {e}")
    if drafts is None or drafts.empty:
        st.info("Nothing waiting. Edit a row or accept an assistant change set first.")
    else:
        st.dataframe(style_table(drafts, brand_col="brand"), hide_index=True, use_container_width=True)
        if st.button("Summarise these changes (via Unity AI Gateway)"):
            try:
                text, model = summarise_for_approver(drafts.head(40).to_csv(index=False))
                st.markdown(text)
                st.caption(f"Answered by {model} through {MODEL_SERVICE}")
            except Exception as e:
                st.warning(f"Gateway summary unavailable: {str(e)[:300]}")
        to_approve = st.selectbox("Approve row", drafts.row_id.tolist())
        if st.button("Approve"):
            err = approve(to_approve, user)
            if err:
                st.error(err)
            else:
                st.success(f"{to_approve} approved. Its content hash is now recorded.")

with tab_history:
    rid = st.text_input("Row id", value="R-0068")
    if rid:
        try:
            hist = run(f"SELECT changed_at, changed_by, field, old_value, new_value, reason, source FROM {table('change_log')} WHERE row_id = :id ORDER BY changed_at DESC", {"id": rid})
            if hist is not None and not hist.empty:
                st.dataframe(hist, hide_index=True, use_container_width=True)
            else:
                st.info("No recorded changes for this row yet.")
        except Exception as e:
            st.error(str(e))

with tab_metrics:
    metrics = load_metrics()
    if metrics is None or metrics.empty:
        st.info("Coming soon...")
    else:
        try:
            render_metrics(metrics)
        except Exception as e:
            st.error(f"Could not draw the metrics: {e}")
