# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Step 1 - Set up your schema and synthetic data (~1 min)
# MAGIC
# MAGIC **Action:** Click **Run all**, then go straight to `02_Build_NBA_with_Genie_Code`.
# MAGIC
# MAGIC This notebook creates, inside **your own schema**:
# MAGIC * `contact_history` - past outreach attempts with whether the lead converted
# MAGIC * `leads_today` - today's open leads waiting for a next action recommendation
# MAGIC
# MAGIC Data is seeded, small (~8k history rows + 400 leads), and designed so a simple model can learn
# MAGIC real patterns (for example: older life-cover leads convert better on morning calls; digital-only
# MAGIC leads prefer SMS/email).

# COMMAND ----------

# MAGIC %run ./_lab_config

# COMMAND ----------

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {FQ_SCHEMA} COMMENT 'Greenstone NBA showcase lab for {USER_EMAIL}. Safe to delete.'"
)
register("schema", FQ_SCHEMA)
print(f"Schema ready: {FQ_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Synthetic contact history + today's leads
# MAGIC No Faker install, no downloads - pure Python so Serverless stays instant.

# COMMAND ----------

import random
from datetime import datetime, timedelta

import pandas as pd

rng = random.Random(RANDOM_SEED)

BRANDS = ["Australian Seniors", "Real Insurance", "OneChoice", "Real Funeral"]
PRODUCTS = ["life_cover", "funeral_cover", "income_protection", "car_insurance"]
SOURCES = ["google_ads", "meta_ads", "bing_ads", "tv", "referral", "comparison_site"]
STATES = ["NSW", "VIC", "QLD", "SA", "WA"]
AGE_BANDS = ["18-34", "35-49", "50-64", "65+"]


def preferred_action(source, product, age_band, state, lead_score):
    """Hidden rules the model should rediscover. Kept simple so Genie Code + sklearn learn them fast."""
    # Digital / younger -> async channels
    if source in ("google_ads", "meta_ads", "bing_ads", "comparison_site") and age_band in ("18-34", "35-49"):
        return "sms" if lead_score < 60 else "email"
    # Older life/funeral -> morning call
    if age_band in ("50-64", "65+") and product in ("life_cover", "funeral_cover"):
        return "call_am"
    # TV and referrals -> phone
    if source in ("tv", "referral"):
        return "call_am" if lead_score >= 55 else "call_pm"
    # High-intent remainder -> afternoon call
    if lead_score >= 70:
        return "call_pm"
    return "email"


def conversion_prob(action, preferred, lead_score, prior_contacts, days_since_last):
    # Large preferred-action lift so ranking quality is obvious in a 30-min lab.
    base = 0.05 + (lead_score / 100.0) * 0.10
    if action == preferred:
        base += 0.50
    elif {action, preferred} <= {"call_am", "call_pm"}:
        base += 0.05
    elif action in ("sms", "email") and preferred in ("sms", "email"):
        base += 0.08
    else:
        base -= 0.10
    if prior_contacts >= 4:
        base -= 0.08
    if days_since_last is not None and days_since_last <= 1:
        base -= 0.05
    return min(max(base, 0.02), 0.88)


history = []
base_day = datetime(2026, 9, 1)
for i in range(N_HISTORY_ROWS):
    brand = rng.choice(BRANDS)
    product = rng.choice(PRODUCTS)
    source = rng.choices(SOURCES, weights=[28, 22, 8, 15, 12, 15])[0]
    state = rng.choices(STATES, weights=[32, 28, 20, 10, 10])[0]
    age_band = rng.choices(AGE_BANDS, weights=[15, 30, 35, 20])[0]
    lead_score = int(min(max(rng.gauss(62, 15), 20), 98))
    prior_contacts = rng.choices([0, 1, 2, 3, 4, 5], weights=[25, 25, 20, 15, 10, 5])[0]
    days_since_last = None if prior_contacts == 0 else rng.randint(0, 21)
    preferred = preferred_action(source, product, age_band, state, lead_score)
    # Mostly try the preferred action, with some exploration noise
    action = preferred if rng.random() < 0.70 else rng.choice(ACTIONS)
    p = conversion_prob(action, preferred, lead_score, prior_contacts, days_since_last)
    converted = 1 if rng.random() < p else 0
    contact_ts = base_day + timedelta(days=rng.randint(0, 75), hours=rng.randint(8, 19), minutes=rng.randint(0, 59))
    history.append(
        {
            "event_id": f"H-{i + 1:05d}",
            "lead_id": f"L-{rng.randint(1, 3500):05d}",
            "brand": brand,
            "product_interest": product,
            "lead_source": source,
            "state": state,
            "age_band": age_band,
            "lead_score": lead_score,
            "prior_contact_count": prior_contacts,
            "days_since_last_contact": days_since_last,
            "action_taken": action,
            "contact_ts": contact_ts,
            "converted": converted,
        }
    )

leads = []
for i in range(N_LEADS_TODAY):
    brand = rng.choice(BRANDS)
    product = rng.choice(PRODUCTS)
    source = rng.choices(SOURCES, weights=[28, 22, 8, 15, 12, 15])[0]
    state = rng.choices(STATES, weights=[32, 28, 20, 10, 10])[0]
    age_band = rng.choices(AGE_BANDS, weights=[15, 30, 35, 20])[0]
    lead_score = int(min(max(rng.gauss(62, 15), 20), 98))
    prior_contacts = rng.choices([0, 1, 2, 3], weights=[40, 30, 20, 10])[0]
    days_since_last = None if prior_contacts == 0 else rng.randint(0, 14)
    leads.append(
        {
            "lead_id": f"T-{i + 1:04d}",
            "brand": brand,
            "product_interest": product,
            "lead_source": source,
            "state": state,
            "age_band": age_band,
            "lead_score": lead_score,
            "prior_contact_count": prior_contacts,
            "days_since_last_contact": days_since_last,
            "created_at": datetime(2026, 11, 18, 8, 0, 0) + timedelta(minutes=i),
        }
    )

history_pdf = pd.DataFrame(history)
leads_pdf = pd.DataFrame(leads)

spark.createDataFrame(history_pdf).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(HISTORY_TABLE)
spark.createDataFrame(leads_pdf).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(LEADS_TABLE)
register("table", HISTORY_TABLE, "contact_history")
register("table", LEADS_TABLE, "leads_today")

print(f"Wrote {len(history_pdf):,} rows -> {HISTORY_TABLE}")
print(f"Wrote {len(leads_pdf):,} rows -> {LEADS_TABLE}")
print("Conversion rate in history:", round(history_pdf["converted"].mean(), 3))
print("Actions:", history_pdf["action_taken"].value_counts().to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Quick sanity check
# MAGIC You should see conversion vary by action - that is the signal the model will learn.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT action_taken,
               COUNT(*) AS contacts,
               ROUND(AVG(converted), 3) AS conversion_rate
        FROM {HISTORY_TABLE}
        GROUP BY action_taken
        ORDER BY conversion_rate DESC
        """
    )
)

display(spark.sql(f"SELECT * FROM {LEADS_TABLE} LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Done
# MAGIC Go to **`02_Build_NBA_with_Genie_Code`**. Use the markdown Genie Code prompts so Genie writes files under `genie/`, then `%run` them.