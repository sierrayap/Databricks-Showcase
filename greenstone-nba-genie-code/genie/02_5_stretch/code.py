# Databricks notebook source
# MAGIC %md
# MAGIC # 02_5_stretch — optional Genie Code file
# MAGIC
# MAGIC Skip unless you finish early. Ask Genie Code to implement **this** notebook, then re-run `%run ./genie/02_5_stretch/code` in `02`.

# COMMAND ----------

# DBTITLE 1,NBA lift vs always-call_pm baseline
# Stretch: business summary - NBA lift vs a naive "always call_pm" baseline (pandas only).
_recs = recs_pdf if "recs_pdf" in globals() else spark.table(RECS_TABLE).toPandas()

if "cand_df" in globals():
    _pm = cand_df.loc[cand_df["action_taken"] == "call_pm", ["lead_id", "conversion_prob"]]
else:  # quick re-score of call_pm with the fitted model
    _x = _recs.drop(columns=["next_best_action", "expected_conversion_prob"]).assign(action_taken="call_pm")
    _x["days_since_last_contact"] = _x["days_since_last_contact"].fillna(-1)
    _cols = ["brand", "product_interest", "lead_source", "state", "age_band", "action_taken",
             "lead_score", "prior_contact_count", "days_since_last_contact"]
    _pm = _x[["lead_id"]].assign(conversion_prob=model.predict_proba(_x[_cols])[:, 1])

lift_df = _recs.merge(_pm.rename(columns={"conversion_prob": "call_pm_prob"}), on="lead_id", how="left")

_n = len(lift_df)
_away = (lift_df["next_best_action"] != "call_pm").mean()
_nba, _base = lift_df["expected_conversion_prob"].mean(), lift_df["call_pm_prob"].mean()

print("=== Greenstone call-centre NBA summary ===")
print(f"Leads scored:                         {_n:,}")
print(f"Recommended away from call_pm:        {_away:.1%}")
print(f"Mean conversion prob - NBA:           {_nba:.3f}")
print(f"Mean conversion prob - always call_pm: {_base:.3f}")
print(f"Lift: +{_nba - _base:.3f} abs ({_nba / _base - 1:+.1%} rel), "
      f"~{(_nba - _base) * _n:.0f} extra expected conversions per {_n:,} leads")