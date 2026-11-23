# Databricks notebook source
# MAGIC %md
# MAGIC # 02_1_explore (presenter solution)

# COMMAND ----------

print("Overall conversion rate:", round(history_pdf["converted"].mean(), 3))
print("\nConversion by action:")
print(history_pdf.groupby("action_taken")["converted"].agg(["count", "mean"]).round(3))
print("\nConversion by lead_source:")
print(history_pdf.groupby("lead_source")["converted"].mean().round(3).sort_values(ascending=False))
print("\nConversion by age_band:")
print(history_pdf.groupby("age_band")["converted"].mean().round(3).sort_values(ascending=False))
print("\nAction x age_band conversion rate:")
print(
    pd.crosstab(
        history_pdf["age_band"],
        history_pdf["action_taken"],
        values=history_pdf["converted"],
        aggfunc="mean",
    ).round(3)
)
