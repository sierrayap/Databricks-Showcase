# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Step 3 - Unity AI Gateway: one governed front door for every LLM call (~10 min)
# MAGIC
# MAGIC Your app also uses an LLM directly: it **summarises pending changes for approvers** before each upload.
# MAGIC That call should never hit a raw model. It goes through a **model service** - a Unity Catalog securable that adds:
# MAGIC
# MAGIC | Control | What you will see |
# MAGIC |---|---|
# MAGIC | Routing + fallback | Primary model with an automatic backup |
# MAGIC | Rate limits | A per-user limit returning HTTP 429 |
# MAGIC | Guardrails | A Unity Catalog SQL function that blocks requests for AI-written ad copy (a compliance rule) |
# MAGIC | Inference table | Every request and response logged to a Delta table |
# MAGIC | Hot swap | Change the model with zero app changes |
# MAGIC
# MAGIC **Action:** run cell by cell. Your Agent Bricks agent app and MAPI app are still deploying in the background. Both send their LLM calls through this model service.

# COMMAND ----------

# MAGIC %pip install -q -U openai
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_lab_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Create your model service

# COMMAND ----------


def destination(name, model):
    return {
        "name": name,
        "destination_type": "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL",
        "pay_per_token_config": {"model": f"models/{model}"},
    }


service_body = {
    "comment": f"MAPI approver-summary LLM for {USER_EMAIL}",
    "config": {
        "routing": {
            "destinations": [{**destination("primary", SUMMARY_MODEL), "traffic_percentage": 100}],
            "fallback": {"destinations": [destination("backup", FALLBACK_MODEL)]},
        },
        "rate_limits": [
            {"key": "RATE_LIMIT_KEY_USER_DEFAULT", "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE", "requests": 5}
        ],
        "inference_table": {"parent": f"schemas/{FQ_SCHEMA}", "table_name_prefix": "gateway"},
    },
}

try:
    service = api("GET", f"/api/2.1/unity-catalog/model-services/{MODEL_SERVICE_FQN}")
    print(f"Reusing model service {MODEL_SERVICE_FQN}")
except Exception:
    service = api_when_ready(
        "POST",
        "/api/2.1/unity-catalog/model-services",
        query={"parent": f"schemas/{FQ_SCHEMA}", "model_service_id": MODEL_SERVICE_ID},
        body=service_body,
    )
    print(f"Created model service {MODEL_SERVICE_FQN}")
register("model_service", MODEL_SERVICE_FQN)
show_link("Model service (Catalog Explorer)", model_service_url())
show_link("AI Gateway Models", ai_gateway_models_url())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Call it like any OpenAI model - the same call the app makes
# MAGIC **Notice:** `model=` is a three-part Unity Catalog name. The app never knows which LLM is behind it.

# COMMAND ----------

from openai import OpenAI

client = OpenAI(api_key=notebook_token(), base_url=AI_GATEWAY_BASE_URL)


def gateway_call(**kwargs):
    """Call the model service. A brand-new service can take a minute to start answering, so wait instead of failing.
    Rate limits (429) are NOT retried - those are the point of step 3.3."""
    return retry_until_ready(
        lambda: client.chat.completions.create(model=MODEL_SERVICE_FQN, **kwargs),
        what="model service",
        timeout_s=300,
        poll_s=10,
    )


def summarise(text):
    return gateway_call(
        messages=[
            {"role": "system", "content": "Summarise pending MAPI ad-config changes for an approver in 3 bullet points. Flag anything off-brand."},
            {"role": "user", "content": text},
        ],
        max_tokens=200,
    )


pending = spark.sql(f"SELECT row_id, brand, product, landing_url, status FROM {FQ_SCHEMA}.mapi_rows WHERE status = 'draft' LIMIT 15").toPandas().to_csv(index=False)
r = summarise(pending)
print(f"Answered by: {r.model}\n")
print(r.choices[0].message.content)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Rate limits
# MAGIC We temporarily drop the limit to **2 requests/minute** (service-wide + per-user), wait a minute for it to apply, then fire a burst.
# MAGIC You should see several `429 Too Many Requests` responses. (Re-running the notebook leaves the limit at 60, so this cell always resets it first.)

# COMMAND ----------

import openai

api_when_ready(
    "PATCH",
    f"/api/2.1/unity-catalog/model-services/{MODEL_SERVICE_FQN}",
    query={"update_mask": "config.rate_limits"},
    body={
        "config": {
            "rate_limits": [
                {"key": "RATE_LIMIT_KEY_SERVICE", "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE", "requests": 2},
                {"key": "RATE_LIMIT_KEY_USER_DEFAULT", "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE", "requests": 2},
            ]
        }
    },
)
print("Limits set to 2 requests/minute (service + per-user). Waiting 60s for them to apply...")
time.sleep(60)

saw_429 = False
for i in range(1, 13):
    try:
        # Call the client directly (not gateway_call) so a 429 is never retried away.
        client.chat.completions.create(model=MODEL_SERVICE_FQN, messages=[{"role": "user", "content": f"Say OK {i}"}], max_tokens=5)
        print(f"Request {i}: 200 OK")
    except openai.RateLimitError:
        saw_429 = True
        print(f"Request {i}: 429 Too Many Requests  <- blocked by Unity AI Gateway")
    except Exception as e:
        msg = str(e).lower()
        if "429" in msg or "rate limit" in msg or "too many requests" in msg or "request_limit_exceeded" in msg:
            saw_429 = True
            print(f"Request {i}: 429 Too Many Requests  <- blocked by Unity AI Gateway")
        else:
            print(f"Request {i}: error - {str(e)[:160]}")

if not saw_429:
    print("No 429 yet. Rate limits can take another minute to apply - wait and re-run this cell.")

# COMMAND ----------

api_when_ready(
    "PATCH",
    f"/api/2.1/unity-catalog/model-services/{MODEL_SERVICE_FQN}",
    query={"update_mask": "config.rate_limits"},
    body={"config": {"rate_limits": [{"key": "RATE_LIMIT_KEY_USER_DEFAULT", "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE", "requests": 60}]}},
)
print("Per-user limit raised to 60 requests/minute for the rest of the lab (can take ~1 minute to apply).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Guardrail: AI must not write funeral or life insurance ad copy
# MAGIC That is a compliance rule in the MAPI design. You will enforce it at the gateway, for every app, with a governed SQL function.
# MAGIC
# MAGIC **Important:** service-policy SQL is transpiled to CEL, so only a small SQL subset is allowed
# MAGIC (`contains`, `lower`, `like`, simple `CASE`). `RLIKE`, `to_json`, subqueries and chains of `OR` are rejected when you attach the policy.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {GUARDRAIL_FN}(event VARIANT)
RETURNS VARIANT
LANGUAGE SQL
COMMENT 'MAPI guardrail: block requests asking an LLM to write ad copy.'
RETURN CASE
  WHEN event:type::string = 'request'
   AND contains(lower(event:context.message::string), 'write ad copy')
  THEN to_variant_object(named_struct(
      'result', 'DENY',
      'reason', 'MAPI compliance: primary ad copy for funeral and life insurance must be written by a person.'
  ))
  ELSE to_variant_object(named_struct('result', 'ALLOW', 'reason', ''))
END
""")
print(f"Policy function created: {GUARDRAIL_FN}")
show_link("Guardrail function", guardrail_url())
try:
    spark.sql(f"GRANT EXECUTE ON FUNCTION {GUARDRAIL_FN} TO `account users`")
except Exception as e:
    print(f"Note: EXECUTE grant on the guardrail skipped ({str(e)[:120]})")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Hands on (2 min): attach the policy in the UI
# MAGIC 1. Left sidebar: **AI Gateway** > **Models** tab > open your service (name printed below).
# MAGIC 2. **Policies** tab > **New policy**.
# MAGIC 3. Name `block_ai_ad_copy`, Guardrail type **Custom**, Implementation **Custom function**, select the function printed below.
# MAGIC 4. **Create policy**, wait ~30 seconds, then run the test cell.

# COMMAND ----------

print(f"Model service : {MODEL_SERVICE_FQN}")
print(f"Function      : {GUARDRAIL_FN}")
show_link("Model service (pick this in AI Gateway > Models)", model_service_url())
show_link("AI Gateway Models tab", ai_gateway_models_url())
show_link("Guardrail function (select this when attaching the policy)", guardrail_url())

# COMMAND ----------

r = summarise("Please write ad copy for Real Funeral cover for the Christmas campaign.")
blocked = getattr(r, "databricks_service_policy", None) or (r.model_extra or {}).get("databricks_service_policy")
print("BLOCKED by guardrail:" if blocked else "Not blocked yet (policy can take a minute to propagate) - reply:")
print(blocked or r.choices[0].message.content)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Hot-swap the model with zero app changes

# COMMAND ----------

api_when_ready(
    "PATCH",
    f"/api/2.1/unity-catalog/model-services/{MODEL_SERVICE_FQN}",
    query={"update_mask": "config.routing.destinations"},
    body={"config": {"routing": {"destinations": [{**destination("primary", FALLBACK_MODEL), "traffic_percentage": 100}]}}},
)
time.sleep(10)
r = summarise(pending)
print(f"Answered by: {r.model}\n")
print(r.choices[0].message.content)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.6 Let your app use the model service
# MAGIC The app's service principal gets `EXECUTE` on the model service - governed in Unity Catalog like a table.

# COMMAND ----------

set_app_resources(app_resources(include_model_service=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.7 Inference table - the audit log
# MAGIC Every call is logged to `gateway_payload` in **your** schema. Rows can take several minutes to land - re-run at the end of the lab.

# COMMAND ----------

INFERENCE_TABLE = f"{FQ_SCHEMA}.gateway_payload"
if spark.catalog.tableExists(INFERENCE_TABLE):
    display(spark.sql(f"SELECT event_time, requester, destination_model, status_code, latency_ms FROM {INFERENCE_TABLE} ORDER BY event_time DESC LIMIT 20"))
else:
    print("Inference table not created yet - check back later.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Next:** `04_App_and_Assistant`. It waits for your assistant and app if they are still finishing.