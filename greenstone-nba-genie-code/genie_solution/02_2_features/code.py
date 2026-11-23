# Databricks notebook source
# MAGIC %md
# MAGIC # 02_2_features (presenter solution)

# COMMAND ----------

train_df = history_pdf.copy()
train_df["days_since_last_contact"] = train_df["days_since_last_contact"].fillna(-1)

cat_cols = ["brand", "product_interest", "lead_source", "state", "age_band", "action_taken"]
num_cols = ["lead_score", "prior_contact_count", "days_since_last_contact"]
feature_cols = cat_cols + num_cols

X = train_df[feature_cols]
y = train_df["converted"].astype(int)
print("X shape:", X.shape, "y shape:", y.shape)
print(y.value_counts())
