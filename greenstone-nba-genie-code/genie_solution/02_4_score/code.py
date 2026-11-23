# Databricks notebook source
# MAGIC %md
# MAGIC # 02_4_score (presenter solution)

# COMMAND ----------

score_df = leads_pdf.copy()
score_df["days_since_last_contact"] = score_df["days_since_last_contact"].fillna(-1)

candidates = []
for action in ACTIONS:
    part = score_df.copy()
    part["action_taken"] = action
    candidates.append(part)
cand_df = pd.concat(candidates, ignore_index=True)

cand_df["conversion_prob"] = model.predict_proba(cand_df[feature_cols])[:, 1]
idx = cand_df.groupby("lead_id")["conversion_prob"].idxmax()
best = cand_df.loc[idx].copy()

recs_pdf = best.rename(
    columns={"action_taken": "next_best_action", "conversion_prob": "expected_conversion_prob"}
)[
    [
        "lead_id",
        "brand",
        "product_interest",
        "lead_source",
        "state",
        "age_band",
        "lead_score",
        "prior_contact_count",
        "days_since_last_contact",
        "next_best_action",
        "expected_conversion_prob",
    ]
].sort_values("expected_conversion_prob", ascending=False)

spark.createDataFrame(recs_pdf).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(RECS_TABLE)
register("table", RECS_TABLE, "nba_recommendations")

print(recs_pdf.head(15))
print("\nRecommended actions:")
print(recs_pdf["next_best_action"].value_counts())
