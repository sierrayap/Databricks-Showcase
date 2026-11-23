# Databricks notebook source
# MAGIC %md
# MAGIC # 02_4_score — Genie Code writes this file
# MAGIC
# MAGIC Do not paste code into `02_Build_NBA_with_Genie_Code`. Ask Genie Code to implement **this** notebook, then re-run `%run ./genie/02_4_score/code` in `02`.

# COMMAND ----------

# DBTITLE 1,Score leads and pick Next Best Action
# 2.4 Score today's leads: try every action per lead, keep the one with the highest conversion probability.
# Expects `model` (02_3_train), `leads_pdf` (2.0), ACTIONS and RECS_TABLE (_lab_config). pandas + sklearn only.
import pandas as pd

for _name in ["model", "leads_pdf", "ACTIONS", "RECS_TABLE"]:
    if _name not in globals():
        raise NameError(f"`{_name}` not found - run _lab_config, 2.0 and 02_3_train first.")

_score_feature_cols = [
    "brand", "product_interest", "lead_source", "state", "age_band", "action_taken",
    "lead_score", "prior_contact_count", "days_since_last_contact",
]

# 1-2. One candidate row per (lead, action); fill missing recency with -1 (same as training)
_actions_df = pd.DataFrame({"action_taken": list(ACTIONS)})
cand_df = leads_pdf.merge(_actions_df, how="cross")
cand_df["days_since_last_contact"] = cand_df["days_since_last_contact"].fillna(-1)

# 3. Conversion probability for every candidate
cand_df["conversion_prob"] = model.predict_proba(cand_df[_score_feature_cols])[:, 1]

# 4-5. Best action per lead
_best_idx = cand_df.groupby("lead_id")["conversion_prob"].idxmax()
_best = cand_df.loc[_best_idx]

# 6. Recommendations table
recs_pdf = (
    _best.rename(columns={"action_taken": "next_best_action", "conversion_prob": "expected_conversion_prob"})
    [[
        "lead_id", "brand", "product_interest", "lead_source", "state", "age_band", "lead_score",
        "prior_contact_count", "days_since_last_contact", "next_best_action", "expected_conversion_prob",
    ]]
    .sort_values("expected_conversion_prob", ascending=False)
    .reset_index(drop=True)
)

# 7. Write to Unity Catalog
spark.createDataFrame(recs_pdf).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(RECS_TABLE)

print(f"Scored {len(cand_df):,} candidates ({cand_df['lead_id'].nunique():,} leads x {len(ACTIONS)} actions)")
print(f"Wrote {len(recs_pdf):,} recommendations to {RECS_TABLE}")

# 8. Top 15
print("\n=== Top 15 recommendations by expected_conversion_prob ===")
print(recs_pdf.head(15)[[
    "lead_id", "brand", "product_interest", "age_band", "lead_score", "next_best_action", "expected_conversion_prob",
]].round({"expected_conversion_prob": 3}).to_string(index=False))

# 9. Action mix
print("\n=== next_best_action value counts ===")
print(recs_pdf["next_best_action"].value_counts().to_string())