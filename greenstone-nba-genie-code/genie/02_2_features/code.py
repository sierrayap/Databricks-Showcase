# Databricks notebook source
# MAGIC %md
# MAGIC # 02_2_features — Genie Code writes this file
# MAGIC
# MAGIC Do not paste code into `02_Build_NBA_with_Genie_Code`. Ask Genie Code to implement **this** notebook, then re-run `%run ./genie/02_2_features/code` in `02`.

# COMMAND ----------

# DBTITLE 1,Prepare NBA training features (pandas)
# 2.2 Prepare training features for the Next Best Action classifier (pandas only, no training here).
# Expects `history_pdf` from the parent notebook; works on a copy so the original is untouched.
if "history_pdf" not in globals():
    raise NameError("`history_pdf` not found - run the 2.0 load cell in the parent notebook first.")

NBA_ACTIONS = ["call_am", "call_pm", "sms", "email"]

cat_cols = ["brand", "product_interest", "lead_source", "state", "age_band", "action_taken"]
num_cols = ["lead_score", "prior_contact_count", "days_since_last_contact"]
feature_cols = cat_cols + num_cols

train_df = history_pdf.copy()
train_df["days_since_last_contact"] = train_df["days_since_last_contact"].fillna(-1)

_unexpected = set(train_df["action_taken"].unique()) - set(NBA_ACTIONS)
if _unexpected:
    print(f"WARNING: unexpected action_taken values: {sorted(_unexpected)}")

X = train_df[feature_cols]
y = train_df["converted"].astype(int)

print(f"X shape: {X.shape} | y shape: {y.shape}")
print(f"Categorical ({len(cat_cols)}): {cat_cols}")
print(f"Numeric ({len(num_cols)}): {num_cols}")
print("\ny value counts:")
print(y.value_counts().sort_index().to_string())