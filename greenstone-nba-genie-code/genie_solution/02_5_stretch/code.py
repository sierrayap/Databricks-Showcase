# Databricks notebook source
# MAGIC %md
# MAGIC # 02_5_stretch (presenter solution)

# COMMAND ----------

call_pm = cand_df[cand_df["action_taken"] == "call_pm"][["lead_id", "conversion_prob"]].rename(
    columns={"conversion_prob": "call_pm_prob"}
)
lift = recs_pdf.merge(call_pm, on="lead_id", how="left")
print("Leads:", len(lift))
print("Share recommended away from call_pm:", round((lift["next_best_action"] != "call_pm").mean(), 3))
print("Mean NBA prob:", round(lift["expected_conversion_prob"].mean(), 3))
print("Mean call_pm prob:", round(lift["call_pm_prob"].mean(), 3))
print("Estimated absolute lift:", round(lift["expected_conversion_prob"].mean() - lift["call_pm_prob"].mean(), 3))
