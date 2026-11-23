# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2 SOLUTION (presenter only)
# MAGIC Runs the completed files under `genie_solution/` instead of the participant stubs in `genie/`.
# MAGIC Use if Genie Code misbehaves live, or to validate the lab the day before.
# MAGIC
# MAGIC Participants should stay on `02_Build_NBA_with_Genie_Code` + `genie/`.

# COMMAND ----------

# MAGIC %run ./_lab_config

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

history_pdf = spark.table(HISTORY_TABLE).toPandas()
leads_pdf = spark.table(LEADS_TABLE).toPandas()
print(f"History rows: {len(history_pdf):,} | Leads today: {len(leads_pdf):,}")

# COMMAND ----------

# MAGIC %run ./genie_solution/02_1_explore/code

# COMMAND ----------

# MAGIC %run ./genie_solution/02_2_features/code

# COMMAND ----------

# MAGIC %run ./genie_solution/02_3_train/code

# COMMAND ----------

# MAGIC %run ./genie_solution/02_4_score/code

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT next_best_action,
               COUNT(*) AS leads,
               ROUND(AVG(expected_conversion_prob), 3) AS avg_prob
        FROM {RECS_TABLE}
        GROUP BY next_best_action
        ORDER BY leads DESC
        """
    )
)

# COMMAND ----------

# MAGIC %run ./genie_solution/02_5_stretch/code
