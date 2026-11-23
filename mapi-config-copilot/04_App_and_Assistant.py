# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Step 4 - Your Agent Bricks agent, behind your MAPI app (~10 min)
# MAGIC
# MAGIC The first cell waits until everything you started in step 1 is ready (it can take a few minutes if you moved fast - just let it run). You will:
# MAGIC 1. Try the agent's code from this notebook and see **which tools it chose**.
# MAGIC 2. Open **your MAPI app** and do a full edit cycle: the Assistant tab calls your **Agent Bricks agent app**, it proposes, you accept, it lands in the approval queue.
# MAGIC 3. See every assistant call **traced in MLflow**.
# MAGIC
# MAGIC **Notice:** Databricks Apps only accept OAuth tokens, and a notebook's token is not one, so the notebook cannot call the agent app over HTTP.
# MAGIC In 4.1 it imports the same `agent/agent_core.py` that the agent app serves, with the same tools and the same AI Gateway model service.
# MAGIC In 4.2 the MAPI app calls the deployed agent app.

# COMMAND ----------

# MAGIC %pip install -q -U openai
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_lab_config

# COMMAND ----------

# Block until the model service answers and both apps are deployed and RUNNING.
wait_for_agents_ready()
app = wait_for_deployment(APP_NAME)
print(f"MAPI app status: {(app.get('app_status') or {}).get('state')}")
show_link("MAPI app (open)", app.get("url") or app_open_url())
show_link("MAPI app (manage)", app_manage_url())
show_link("Agent app (Agent Bricks, manage)", agent_app_manage_url())
show_link("Model service (AI Gateway)", model_service_url())
print(f"Rule book: Unity Catalog function {FQ_SCHEMA}.lookup_rules (plus the brand_rules volume)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Try the agent's code from the notebook
# MAGIC This runs the same agent code your agent app serves. Change the request and re-run. **Notice** which tool was called (`search_rows`, `propose_change_set`, `find_issues`, `lookup_rules`).

# COMMAND ----------


def show(question):
    answer, raw = ask_agent(question)
    tools_called = [t.get("tool") for t in (raw.get("tools_called") or [])]
    print(f"Q: {question}\nTools: {tools_called or '(none)'}\n")
    print(answer[:2500])
    print("-" * 80)


show("How many approved rows does Real Funeral have, and which campaigns are they in?")
show("Change the URL for all Real t001 rows to the new quote page.")

# COMMAND ----------

show("Approve all the Real Funeral drafts please.")
show("Show me active rows where the copy does not match the row's brand.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Hands on in your app (6 min)
# MAGIC Open the link printed below, then:
# MAGIC
# MAGIC 1. **Assistant** tab (served by your Agent Bricks agent app): ask `Change the landing page URL for all Real Funeral t001 rows to https://www.realfuneral.example/funeral-cover/quote`.
# MAGIC    Review the preview, untick one row, click **Save accepted rows as drafts**.
# MAGIC 2. **Approval queue** tab: your drafts are there with the reason (your original request). Click **Summarise these changes** - that call goes through your Unity AI Gateway model service.
# MAGIC 3. Try to **Approve** one of your own drafts - it is refused, because you edited it last. Then pick a row in the **Approve row** dropdown whose `last_edited_by` is someone else (the table above the dropdown shows it) and click **Approve**.
# MAGIC 4. **Row history** tab: open one of the rows you changed.
# MAGIC 5. Sidebar: **Pause uploads**. MEX can stop the next Meta upload without an engineer.
# MAGIC
# MAGIC **Notice:** the app has no Databricks UI. Marketing users only need the **consumer access** entitlement to use it.
# MAGIC
# MAGIC **Notice:** the app keeps your edits in **PGlite**, an in-memory Postgres running inside the app, so they do not appear in Unity Catalog. The **caption** under the title says so.

# COMMAND ----------

displayHTML(f'<a href="{(get_app() or {}).get("url")}" target="_blank" style="font-size:20px">Open your MAPI app</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 What your app remembers
# MAGIC Open the **Row history** tab in your app. Every change, approval and pause is recorded there.
# MAGIC
# MAGIC **Notice:** that history lives in PGlite, in the app's memory. Restart the app (or redeploy it) and it starts again from the seed data.
# MAGIC That is fine for this session, which is about the agent. Because PGlite speaks Postgres, the next session can swap it for a persistent Lakebase database without rewriting the app's SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Every assistant call is traced in MLflow
# MAGIC Asking the assistant from this notebook is traced in your MLflow experiment in step 5.
# MAGIC Open a trace for the Real Funeral request and find the `propose_change_set` tool call.

# COMMAND ----------

mlflow_exp = registered("mlflow_experiment")
exp_id = mlflow_exp[0] if mlflow_exp else None
if not exp_id:
    print("Traces show up after you run 05_Evaluate_with_MLflow (it creates the experiment).")
else:
    displayHTML(f'<a href="{WORKSPACE_HOST}/ml/experiments/{exp_id}/traces" target="_blank" style="font-size:18px">Open the assistant traces in MLflow</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC **Next:** `05_Evaluate_with_MLflow`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Optional: try a rule-book question
# MAGIC The agent looks up policy from `lookup_rules` (and from the rule book in its instructions). Re-run if you want to see that tool fire.

# COMMAND ----------

show("Which landing domain is allowed for Real Insurance?")