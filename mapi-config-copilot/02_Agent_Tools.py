# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Step 2 - The assistant's tools, and deploying your apps (~7 min)
# MAGIC
# MAGIC An agent is only as safe as its tools. The MAPI custom agent gets exactly four Unity Catalog functions:
# MAGIC
# MAGIC | Tool | What it does | What it can never do |
# MAGIC |---|---|---|
# MAGIC | `search_rows` | Count and list rows | Change anything |
# MAGIC | `propose_change_set` | Build a **preview** of a bulk change | Save it - a person accepts it in the app |
# MAGIC | `find_issues` | Quality checks (off-brand URLs, wrong-brand copy, changed approved rows) | Fix them |
# MAGIC | `lookup_rules` | Read the MAPI rule book | Change anything |
# MAGIC
# MAGIC There is **no approve, publish or upload tool**, so the assistant cannot do those things however it is prompted.
# MAGIC
# MAGIC **Action:** run cell by cell. The last cell deploys your Agent Bricks agent app and your MAPI app in the background.

# COMMAND ----------

# MAGIC %run ./_lab_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Count rows - exactly what the assistant will see

# COMMAND ----------

r = spark.sql(f"SELECT {FQ_SCHEMA}.search_rows('Real Funeral', '', '', '', 'approved') AS out").first().out
out = json.loads(r)
print(f"Approved Real Funeral rows: {out['match_count']}")
print(json.dumps(out["rows"][:2], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Propose a bulk change - and watch the guardrails inside the tool
# MAGIC **Notice:** "Real" matches two brands, so the tool refuses and tells the assistant to ask. Then a precise request returns a preview with old and new values.

# COMMAND ----------

def propose(brand, product, campaign, asset, field, value):
    q = f"SELECT {FQ_SCHEMA}.propose_change_set(:f, :v, :b, :p, :c, :a) AS out"
    return json.loads(spark.sql(q, args={"b": brand, "p": product, "c": campaign, "a": asset, "f": field, "v": value}).first().out)


ambiguous = propose("Real", "", "", "t001", "landing_url", "https://www.realfuneral.example/funeral-cover/quote")
print("Ambiguous request ->", ambiguous.get("blocked_reason"), ambiguous["brands_matched"])

precise = propose("Real Funeral", "", "", "t001", "landing_url", "https://www.realfuneral.example/funeral-cover/quote")
print(f"\nPrecise request -> {precise['row_count']} rows, blocked: {precise.get('blocked_reason')}")
display(spark.createDataFrame(precise["proposed_changes"]))

# COMMAND ----------

# MAGIC %md
# MAGIC Try to make the tool do something it must not. Both are refused **by the tool itself**, not by the prompt.

# COMMAND ----------

print("Write copy ->", propose("OneChoice", "funeral_cover", "", "", "primary_copy", "Act now before it is too late!").get("blocked_reason"))
print("Approve    ->", propose("Real Funeral", "", "", "", "status", "approved").get("blocked_reason"))
off_brand = propose("OneChoice", "life_cover", "2026-12-christmas", "t001", "landing_url", "https://www.realinsurance.example/quote")
print("Off-brand  ->", (off_brand.get("proposed_changes") or [{}])[0].get("url_on_brand_domain"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Find the 5 November incident
# MAGIC Row `R-0068` was an approved Real Funeral ad whose brand was switched to My Comparison Life during testing.

# COMMAND ----------

issues = json.loads(spark.sql(f"SELECT {FQ_SCHEMA}.find_issues('') AS out").first().out)
print(f"{issues['issue_count']} rows with issues")
display(spark.createDataFrame([(i["row_id"], i["brand"], i["status"], ", ".join(i["issues"])) for i in issues["rows"]], "row_id STRING, brand STRING, status STRING, issues STRING"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Deploy your two apps (runs in the background, ~3-5 min)
# MAGIC Compute for both started in step 1. This cell:
# MAGIC 1. **Agent app** (Agent Bricks): gives its service principal the SQL warehouse, `EXECUTE` on the **AI Gateway model service**,
# MAGIC    and read + execute on your schema (it runs the four tools). Then it deploys `./agent`: a `DurableAgentServer` serving the custom agent at `/api/invocations`.
# MAGIC 2. **MAPI app** (UI): gives its service principal the warehouse, the model service (approver summaries) and your schema,
# MAGIC    plus **`CAN_USE` on the agent app** so the Assistant tab can call it. Then it deploys `./app` with the agent's URL.
# MAGIC
# MAGIC ```
# MAGIC MAPI app (Assistant tab) --HTTP, MAPI app service principal--> agent app /api/invocations --> UC function tools
# MAGIC                                                                     |--every LLM call--> AI Gateway model service
# MAGIC ```
# MAGIC
# MAGIC Then go to **`03_Unity_AI_Gateway`**. You open the MAPI app in step 4.

# COMMAND ----------

wait_for_app_compute(AGENT_APP_NAME)
set_app_resources(app_resources(include_model_service=True), name=AGENT_APP_NAME)
agent_sp = grant_schema_to_app(AGENT_APP_NAME, privileges="USE SCHEMA, SELECT, EXECUTE")
print(f"Agent app service principal {agent_sp} granted USE / SELECT / EXECUTE on {FQ_SCHEMA}")
deploy_agent_app()
register("workspace_file", AGENT_SOURCE_PATH)

wait_for_app_compute(APP_NAME)
set_app_resources(app_resources(include_model_service=True))
app_sp = grant_schema_to_app(APP_NAME)
print(f"MAPI app service principal {app_sp} granted USE / SELECT / MODIFY / EXECUTE on {FQ_SCHEMA}")
grant_agent_to_app()
print(f"MAPI app service principal {app_sp} granted CAN_USE on {AGENT_APP_NAME}")
deploy_app(app_env("pglite"))
register("workspace_file", APP_SOURCE_PATH)

print(f"\nBoth apps are deploying (~3-5 min).")
show_link("Agent app (Agent Bricks, manage)", agent_app_manage_url())
show_link("MAPI app (manage)", app_manage_url())
show_link("MAPI app (open for users)", app_open_url() or "(URL appears once compute is ACTIVE - re-run this cell or check 04)")