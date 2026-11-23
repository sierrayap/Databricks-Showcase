# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Step 1 - Set up your lab and start everything slow (~4 min, then it runs in the background)
# MAGIC
# MAGIC **Action:** Click **Run all**, then go straight to `02_Agent_Tools`. Do not wait here.
# MAGIC
# MAGIC This notebook creates, inside **your own schema**:
# MAGIC * `mapi_rows` - a synthetic copy of the `mapi_config_new_textads` sheet (Meta text-ad config), including the 5 November incident row
# MAGIC * `brand_product_master` - which products each brand sells, and its allowed landing-page domain
# MAGIC * `change_log` and `upload_controls` - audit trail and the `uploads_paused` switch
# MAGIC * `brand_rules` volume and `rule_book` table - the MAPI rule book the custom agent can look up
# MAGIC * 4 Unity Catalog functions - the only tools the assistant is allowed to use
# MAGIC
# MAGIC It then **kicks off the slow resources** so they are ready when you need them:
# MAGIC
# MAGIC | Resource | Ready in | First used in |
# MAGIC |---|---|---|
# MAGIC | AI Gateway model service (every LLM call goes through it) | ~1 min | `02` |
# MAGIC | Agent Bricks agent app compute (`DurableAgentServer`) | ~3 min | `02` deploys the code |
# MAGIC | MAPI UI app compute | ~3 min | `02` deploys the code |

# COMMAND ----------

# MAGIC %run ./_lab_config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ_SCHEMA} COMMENT 'MAPI showcase lab for {USER_EMAIL}. Safe to delete.'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ_SCHEMA}.{VOLUME}")

# Shared schema for the gateway guardrail (one function per participant, named with their username).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {POLICY_SCHEMA} COMMENT 'Shared MAPI guardrail policies, one function per participant.'")
try:
    spark.sql(f"GRANT USE SCHEMA, CREATE FUNCTION ON SCHEMA {POLICY_SCHEMA} TO `account users`")
except Exception as e:
    print(f"Note: could not grant CREATE FUNCTION on {POLICY_SCHEMA} to account users ({str(e)[:120]}). Ask the presenter.")
register("schema", FQ_SCHEMA)
print(f"Schema ready: {FQ_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Synthetic MAPI data
# MAGIC Seeded, so everyone gets identical rows and the evaluation in step 5 has known answers.
# MAGIC Brand names follow the design doc; every URL uses a fake `.example` domain.

# COMMAND ----------

import random
from datetime import datetime, timedelta

rng = random.Random(7)

BRANDS = {
    "Real Funeral": ("realfuneral.example", ["funeral_cover"]),
    "Real Insurance": ("realinsurance.example", ["life_cover", "income_protection", "car_insurance"]),
    "OneChoice": ("onechoice.example", ["life_cover", "funeral_cover"]),
    "My Comparison Life": ("mycomparisonlife.example", ["life_cover_compare"]),
}
PRODUCT_COPY = {
    "funeral_cover": "{brand} Funeral Cover helps your family with funeral costs. Cover from $3,000 to $15,000. Get a quote today.",
    "life_cover": "{brand} Life Cover pays a lump sum to your loved ones. Apply over the phone in minutes.",
    "income_protection": "{brand} Income Protection can replace up to 70% of your income if you cannot work due to illness or injury.",
    "car_insurance": "{brand} Car Insurance with flexible excess options. Compare our comprehensive cover.",
    "life_cover_compare": "{brand} compares life cover from leading Australian insurers. See what suits you.",
}
CAMPAIGNS = ["2026-10-spring", "2026-11-always-on", "2026-12-christmas"]
ASSETS = [f"t00{i}" for i in range(1, 7)]
EDITORS = ["rachel.mex@example.com", "hannah.mex@example.com", "sam.mex@example.com"]
APPROVERS = ["amadea.dp@example.com", "maria.dp@example.com"]


def content_hash(brand, product, primary_copy, landing_url, text_asset_id):
    return hashlib.sha256("|".join([brand, product, primary_copy, landing_url, text_asset_id]).encode()).hexdigest()


master = [(b, p, d) for b, (d, products) in BRANDS.items() for p in products]

rows = []
base = datetime(2026, 9, 20)
for brand, (domain, products) in BRANDS.items():
    for product in products:
        for campaign in CAMPAIGNS:
            for asset in ASSETS:
                copy = PRODUCT_COPY[product].format(brand=brand)
                url = f"https://www.{domain}/{product.replace('_', '-')}/{campaign}?asset={asset}"
                if campaign == "2026-10-spring":
                    status, uploaded = "approved", base + timedelta(days=rng.randint(5, 20))
                elif campaign == "2026-11-always-on":
                    status, uploaded = rng.choice(["approved", "approved", "draft"]), None
                else:
                    status, uploaded = rng.choice(["draft", "draft", "approved"]), None
                approved = status == "approved"
                rows.append({
                    "brand": brand, "product": product, "campaign": campaign, "text_asset_id": asset,
                    "primary_copy": copy, "landing_url": url, "status": status,
                    "approved_hash": content_hash(brand, product, copy, url, asset) if approved else None,
                    "approved_by": rng.choice(APPROVERS) if approved else None,
                    "approved_at": base + timedelta(days=rng.randint(1, 30)) if approved else None,
                    "last_edited_by": rng.choice(EDITORS),
                    "updated_at": base + timedelta(days=rng.randint(0, 25)),
                    "row_version": 1,
                    "last_uploaded_at": uploaded,
                })

# Row 68: the 5 November incident. An approved Real Funeral row had brand and product changed to
# My Comparison Life during testing. Copy and URL still say Real Funeral, and the approval stayed in place.
orig = dict(brand="Real Funeral", product="funeral_cover", campaign="2026-11-always-on", text_asset_id="t005")
orig_copy = PRODUCT_COPY["funeral_cover"].format(brand="Real Funeral")
orig_url = "https://www.realfuneral.example/funeral-cover/2026-11-always-on?asset=t005"
rows[67] = {
    **orig, "brand": "My Comparison Life", "product": "life_cover_compare",
    "primary_copy": orig_copy, "landing_url": orig_url, "status": "approved",
    "approved_hash": content_hash(orig["brand"], orig["product"], orig_copy, orig_url, orig["text_asset_id"]),
    "approved_by": "amadea.dp@example.com", "approved_at": datetime(2026, 10, 30, 9, 12),
    "last_edited_by": "data.engineer@example.com", "updated_at": datetime(2026, 11, 5, 14, 3),
    "row_version": 2, "last_uploaded_at": None,
}
# A draft with an off-brand URL, so validation has something else to catch.
rows[102]["landing_url"] = rows[102]["landing_url"].replace(BRANDS[rows[102]["brand"]][0], "realinsurance.example") if rows[102]["brand"] != "Real Insurance" else rows[102]["landing_url"].replace("realinsurance.example", "onechoice.example")
rows[102]["status"], rows[102]["approved_hash"], rows[102]["approved_by"], rows[102]["approved_at"] = "draft", None, None, None

for i, r in enumerate(rows, start=1):
    r["row_id"] = f"R-{i:04d}"

ROW_SCHEMA = (
    "row_id STRING, brand STRING, product STRING, campaign STRING, text_asset_id STRING, primary_copy STRING, "
    "landing_url STRING, status STRING, approved_hash STRING, approved_by STRING, approved_at TIMESTAMP, "
    "last_edited_by STRING, updated_at TIMESTAMP, row_version INT, last_uploaded_at TIMESTAMP"
)
cols = [c.split()[0] for c in ROW_SCHEMA.split(", ")]
spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], ROW_SCHEMA).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{FQ_SCHEMA}.mapi_rows")
spark.createDataFrame(master, "brand STRING, product STRING, allowed_url_domain STRING").write.mode("overwrite").saveAsTable(
    f"{FQ_SCHEMA}.brand_product_master"
)
spark.sql(f"""CREATE OR REPLACE TABLE {FQ_SCHEMA}.change_log (
  change_id STRING, row_id STRING, field STRING, old_value STRING, new_value STRING,
  changed_by STRING, changed_at TIMESTAMP, reason STRING, source STRING)""")
spark.sql(f"CREATE OR REPLACE TABLE {FQ_SCHEMA}.upload_controls (control STRING, value STRING, updated_by STRING, updated_at TIMESTAMP)")
spark.sql(f"INSERT INTO {FQ_SCHEMA}.upload_controls VALUES ('uploads_paused', 'false', 'setup', current_timestamp())")

# Every seeded draft gets one change_log entry from the editor who last touched it. The approval queue only
# lists drafts that have a logged change, so without this the queue would be empty for other editors' drafts.
seed_log = [
    (f"seed-{r['row_id']}", r["row_id"], "landing_url", r["landing_url"], r["landing_url"], r["last_edited_by"],
     r["updated_at"], "Seeded draft waiting for approval", "editor")
    for r in rows if r["status"] == "draft"
]
spark.createDataFrame(seed_log, "change_id STRING, row_id STRING, field STRING, old_value STRING, new_value STRING, "
                      "changed_by STRING, changed_at TIMESTAMP, reason STRING, source STRING").write.mode("append").saveAsTable(
    f"{FQ_SCHEMA}.change_log")
print(f"{len(seed_log)} seeded drafts are in the approval queue, each from its last editor")

print(f"{len(rows)} MAPI rows, {len(master)} brand/product pairs")
display(spark.sql(f"SELECT row_id, brand, product, campaign, text_asset_id, status, landing_url FROM {FQ_SCHEMA}.mapi_rows WHERE row_id = 'R-0068'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 The assistant's tools: Unity Catalog functions
# MAGIC These are the **only** things the assistant can do (the rule-book lookup is added with the documents in the next cell). There is no approve, publish or upload tool, so it cannot do those things however it is asked.
# MAGIC The `COMMENT`s matter: the custom agent reads them to decide when to call each tool.

# COMMAND ----------

FILTERS = """
    (coalesce(brand_filter, '') = '' OR lower(r.brand) LIKE concat('%', lower(brand_filter), '%'))
    AND (coalesce(product_filter, '') = '' OR r.product = product_filter)
    AND (coalesce(campaign_filter, '') = '' OR r.campaign = campaign_filter)
    AND (coalesce(text_asset_filter, '') = '' OR r.text_asset_id = text_asset_filter)
"""
PARAMS = """
  brand_filter STRING DEFAULT '' COMMENT 'Brand name or part of it, e.g. Real Funeral. Empty string for any brand.',
  product_filter STRING DEFAULT '' COMMENT 'Product code: funeral_cover, life_cover, income_protection, car_insurance or life_cover_compare. Empty string for any.',
  campaign_filter STRING DEFAULT '' COMMENT 'Campaign: 2026-10-spring, 2026-11-always-on or 2026-12-christmas. Empty string for any.',
  text_asset_filter STRING DEFAULT '' COMMENT 'Text asset id t001 to t006. Empty string for any.'
"""
DOMAINS = f"(SELECT brand, first(allowed_url_domain) AS allowed_url_domain FROM {FQ_SCHEMA}.brand_product_master GROUP BY brand)"

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ_SCHEMA}.search_rows({PARAMS},
  status_filter STRING DEFAULT '' COMMENT 'draft, approved or archived. Empty string for any.')
RETURNS STRING
COMMENT 'Search MAPI text-ad rows. Returns match_count, brands_matched and up to 25 matching rows (row_id, brand, product, campaign, text_asset_id, status, landing_url, primary_copy). Use for counting rows and answering questions about the config.'
RETURN (
  SELECT to_json(named_struct(
    'match_count', count(*),
    'brands_matched', array_sort(collect_set(r.brand)),
    'rows', slice(collect_list(named_struct('row_id', r.row_id, 'brand', r.brand, 'product', r.product, 'campaign', r.campaign,
      'text_asset_id', r.text_asset_id, 'status', r.status, 'landing_url', r.landing_url, 'primary_copy', r.primary_copy)), 1, 25)))
  FROM {FQ_SCHEMA}.mapi_rows r
  WHERE {FILTERS} AND (coalesce(status_filter, '') = '' OR r.status = status_filter)
)
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ_SCHEMA}.propose_change_set(
  field_name STRING COMMENT 'Field to change: landing_url, campaign, or status (status can only be set to archived).',
  new_value STRING COMMENT 'The new value for that field.',
  {PARAMS})
RETURNS STRING
COMMENT 'Proposes (does NOT save) a bulk change to MAPI rows. Returns row_count, brands_matched, blocked_reason (if the change is not allowed or is ambiguous) and proposed_changes with old and new values, whether a new URL is on the brand domain, and whether the row will need re-approval. Nothing is changed until a person accepts it in the app.'
RETURN (
  SELECT to_json(named_struct(
    'request', named_struct('field', field_name, 'new_value', new_value),
    'row_count', count(*),
    'brands_matched', array_sort(collect_set(c.brand)),
    'blocked_reason', CASE
      WHEN field_name NOT IN ('landing_url', 'campaign', 'status') THEN 'The assistant can only change landing_url, campaign, or archive rows. Primary copy must be written by a person.'
      WHEN field_name = 'status' AND new_value <> 'archived' THEN 'The assistant can only archive rows. Approval happens in the approval queue by Digital Performance.'
      WHEN count(*) = 0 THEN 'No rows matched these filters.'
      WHEN count(*) > 200 THEN 'More than 200 rows matched. Split the request into smaller change sets.'
      WHEN size(collect_set(c.brand)) > 1 AND coalesce(brand_filter, '') <> '' THEN 'The brand filter matched more than one brand. Ask the user which brand they mean.'
      ELSE NULL END,
    'proposed_changes', slice(collect_list(named_struct(
      'row_id', c.row_id, 'brand', c.brand, 'product', c.product, 'campaign', c.campaign, 'text_asset_id', c.text_asset_id,
      'field', c.change_field, 'old_value', c.old_value, 'new_value', c.change_value,
      'url_on_brand_domain', c.url_on_brand_domain, 'requires_reapproval', c.requires_reapproval)), 1, 200)))
  FROM (
    -- Parameters are projected per row here, so the aggregates above only reference columns.
    SELECT r.row_id, r.brand, r.product, r.campaign, r.text_asset_id,
      field_name AS change_field,
      new_value AS change_value,
      CASE field_name WHEN 'landing_url' THEN r.landing_url WHEN 'campaign' THEN r.campaign WHEN 'status' THEN r.status END AS old_value,
      CASE WHEN field_name = 'landing_url' THEN contains(lower(new_value), d.allowed_url_domain) ELSE true END AS url_on_brand_domain,
      r.status = 'approved' AS requires_reapproval
    FROM {FQ_SCHEMA}.mapi_rows r LEFT JOIN {DOMAINS} d ON r.brand = d.brand
    WHERE {FILTERS} AND r.status <> 'archived'
  ) c
)
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ_SCHEMA}.find_issues(
  brand_filter STRING DEFAULT '' COMMENT 'Brand name or part of it. Empty string for all brands.')
RETURNS STRING
COMMENT 'Quality check across active MAPI rows. Finds rows whose product is not sold by the brand, whose landing URL is not on the brand domain, whose copy does not mention the row brand, or that are approved but whose content changed after approval. Returns issue_count and the rows with their issues.'
RETURN (
  SELECT to_json(named_struct('issue_count', count(*),
    'rows', slice(collect_list(named_struct('row_id', row_id, 'brand', brand, 'product', product, 'status', status,
      'landing_url', landing_url, 'issues', issues)), 1, 50)))
  FROM (
    SELECT r.row_id, r.brand, r.product, r.status, r.landing_url,
      filter(array(
        CASE WHEN m.product IS NULL THEN 'product is not sold by this brand' END,
        CASE WHEN NOT contains(lower(r.landing_url), coalesce(d.allowed_url_domain, '~')) THEN 'landing URL is not on the brand domain' END,
        CASE WHEN NOT contains(lower(r.primary_copy), lower(r.brand)) THEN 'primary copy does not mention the row brand' END,
        CASE WHEN r.status = 'approved' AND r.approved_hash <> sha2(concat_ws('|', r.brand, r.product, r.primary_copy, r.landing_url, r.text_asset_id), 256)
             THEN 'approved content changed after approval' END
      ), x -> x IS NOT NULL) AS issues
    FROM {FQ_SCHEMA}.mapi_rows r
    LEFT JOIN {FQ_SCHEMA}.brand_product_master m ON r.brand = m.brand AND r.product = m.product
    LEFT JOIN {DOMAINS} d ON r.brand = d.brand
    WHERE r.status <> 'archived'
      AND (coalesce(brand_filter, '') = '' OR lower(r.brand) LIKE concat('%', lower(brand_filter), '%'))
  ) WHERE size(issues) > 0
)
""")
print("Tools created: search_rows, propose_change_set, find_issues")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 The MAPI rule book (documents the custom agent can look up)

# COMMAND ----------

docs = {
    "01_mapi_overview.md": """# MAPI text-ad configuration overview
MAPI is the configuration that drives Meta (Facebook and Instagram) text ads. Each row is one ad variant.
Columns: row_id, brand, product, campaign, text_asset_id, primary_copy, landing_url, status, approved_by, approved_at, last_edited_by, row_version, last_uploaded_at.
Statuses: draft (being edited), approved (signed off by Digital Performance), archived (retired, never uploaded).
The upload job sends approved rows to Meta. It runs on a schedule and can be paused with the uploads_paused control.
""",
    "02_brand_product_catalogue.md": """# Brand and product catalogue
Real Funeral sells funeral_cover only. Allowed landing domain: realfuneral.example.
Real Insurance sells life_cover, income_protection and car_insurance. Allowed landing domain: realinsurance.example.
OneChoice sells life_cover and funeral_cover. Allowed landing domain: onechoice.example.
My Comparison Life sells life_cover_compare only. It is a comparison service, not an insurer. Allowed landing domain: mycomparisonlife.example.
"Real" on its own is ambiguous: it can mean Real Funeral or Real Insurance. Always confirm which brand is meant.
""",
    "03_landing_url_rules.md": """# Landing URL rules
Every landing_url must be on the allowed domain for the row's brand. Off-brand URLs are a compliance breach.
URLs must use https.
The quote page for each product follows the pattern https://www.<brand domain>/<product>/quote.
Changing a landing_url on an approved row sends the row back to draft and it must be re-approved.
""",
    "04_approval_and_upload.md": """# Approval and upload workflow
MEX editors create and edit drafts. Digital Performance approvers (the mapi_approvers group) approve rows.
An approver cannot approve a row they edited last.
Approval is bound to content: approval records a hash of brand, product, primary_copy, landing_url and text_asset_id. If any of these change, the approval is void and the row returns to draft.
Before every upload, approvers receive a diff of new and changed rows.
Any MEX or Digital Performance user can pause uploads by setting uploads_paused to true. No engineer is needed.
Engineers test only on a test copy (Lakebase test branch), never on live rows.
""",
    "05_compliance_funeral_life.md": """# Compliance rules for funeral and life insurance advertising
Do not use pressure language such as "act now before it is too late" or "don't leave your family with nothing".
Do not claim cover is guaranteed or free.
Primary copy must be written or approved by a person. AI must not write primary ad copy for funeral or life insurance.
Copy for one brand must never appear on another brand's rows. Copy must name the row's own brand.
""",
    "06_campaign_and_asset_naming.md": """# Campaign and asset naming
Campaigns are named YYYY-MM-theme, for example 2026-12-christmas.
Text assets are numbered t001 to t006 within each campaign.
To create a new campaign, duplicate existing approved rows as drafts and change the campaign field. Duplicates must be re-approved.
Rows for campaigns that have ended should be archived, not deleted, so the history is kept.
""",
    "07_assistant_rules.md": """# Rules for the MAPI assistant
The assistant proposes changes only. It saves nothing until the user accepts the preview, and every accepted change goes to the approval queue.
The assistant has no approve, publish or upload capability.
Change sets larger than 200 rows must be split.
The reason recorded for an assistant change is the user's original request.
""",
}
for filename, content in docs.items():
    dbutils.fs.put(f"{VOLUME_PATH}/{filename}", content, overwrite=True)
spark.createDataFrame(list(docs.items()), "filename STRING, content STRING").write.mode("overwrite").saveAsTable(
    f"{FQ_SCHEMA}.rule_book"
)
spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ_SCHEMA}.lookup_rules(
  topic STRING DEFAULT '' COMMENT 'Keyword such as brand, landing URL, approval, compliance, campaign or assistant. Empty string returns every rule-book section.')
RETURNS STRING
COMMENT 'Look up the MAPI rule book. Use for policy questions: allowed products and domains, landing URL rules, approval and upload workflow, funeral and life-insurance compliance, campaign naming, assistant limits.'
RETURN (
  SELECT to_json(named_struct(
    'match_count', count(*),
    'sections', collect_list(named_struct('filename', filename, 'content', content))))
  FROM {FQ_SCHEMA}.rule_book
  WHERE coalesce(topic, '') = ''
     OR lower(filename) LIKE concat('%', lower(topic), '%')
     OR lower(content) LIKE concat('%', lower(topic), '%')
)
""")
print(f"{len(docs)} rule documents in {VOLUME_PATH} and {FQ_SCHEMA}.rule_book (lookup_rules tool ready)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 AI Gateway model service (the agent's LLM route)

# COMMAND ----------

ensure_model_service()
register("model_service", MODEL_SERVICE_FQN)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Kick off your two Databricks Apps
# MAGIC * **Agent app** - your Agent Bricks custom agent, served by the Agent Bricks runtime (`DurableAgentServer`).
# MAGIC * **MAPI app** - the editor UI your MEX users open. Its Assistant tab calls the agent app.

# COMMAND ----------

ensure_app(AGENT_APP_NAME, f"MAPI Agent Bricks custom agent for {USER_EMAIL} (showcase lab)")
ensure_app(APP_NAME, f"MAPI config editor UI for {USER_EMAIL} (showcase lab)")
print(f"Apps {AGENT_APP_NAME} and {APP_NAME}: compute starting (~3 min). Code is deployed in step 2.")
show_link("Agent app (manage)", agent_app_manage_url())
show_link("MAPI app (manage)", app_manage_url())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Done - everything slow is now warming up
# MAGIC Go to **`02_Agent_Tools`** now. You will meet your assistant in the app in step 4, by which time both apps should be deployed.