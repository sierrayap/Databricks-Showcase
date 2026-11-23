# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Step 2 - Build Next Best Action with Genie Code (~20-25 min)
# MAGIC
# MAGIC **How to use this notebook**
# MAGIC 1. Attach to **Serverless**.
# MAGIC 2. Run the setup cells until the first `%run ./genie/...` cell.
# MAGIC 3. For each step: open **Genie Code**, paste the prompt from the markdown cell **exactly**.
# MAGIC    Genie Code should **create / overwrite the file named in the prompt** (under `genie/`), not this notebook.
# MAGIC 4. Re-run the `%run` cell. Databricks pulls in that file, so you can open the `genie/` folder to see the diff.
# MAGIC 5. To revert a step, restore the stub `code` notebook in that folder (or clone the lab folder again).
# MAGIC
# MAGIC If something errors, paste the traceback into Genie Code and ask it to **fix that same file** (keep pandas + sklearn, no clusters/GPUs/endpoints).
# MAGIC
# MAGIC **Goal:** for each lead in `leads_today`, recommend one of `call_am`, `call_pm`, `sms`, `email` and write results to `nba_recommendations`.

# COMMAND ----------

# MAGIC %run ./_lab_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.0 Load the lab tables (complete - just run this)

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import HistGradientBoostingClassifier

history_pdf = spark.table(HISTORY_TABLE).toPandas()
leads_pdf = spark.table(LEADS_TABLE).toPandas()

print(f"History rows: {len(history_pdf):,} | Leads today: {len(leads_pdf):,}")
print("Actions available:", ACTIONS)
history_pdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Explore what drives conversion
# MAGIC
# MAGIC **Genie Code writes:** `genie/02_1_explore/code`
# MAGIC
# MAGIC #### Genie Code prompt — copy everything inside the box
# MAGIC
# MAGIC ```text
# MAGIC Create or overwrite the Databricks notebook at genie/02_1_explore/code in this lab folder.
# MAGIC Do not edit 02_Build_NBA_with_Genie_Code. Put ALL implementation in genie/02_1_explore/code.
# MAGIC
# MAGIC Using the pandas DataFrame `history_pdf` already in the parent notebook session after %run, write fast exploratory analysis for a Greenstone insurance Next Best Action lab.
# MAGIC
# MAGIC Requirements:
# MAGIC - Stay on pandas only. Do not create Spark clusters, jobs, GPUs, or model serving endpoints.
# MAGIC - Print overall conversion rate.
# MAGIC - Show conversion rate by `action_taken`.
# MAGIC - Show conversion rate by `lead_source` and by `age_band`.
# MAGIC - Show a crosstab of `action_taken` vs `age_band` with conversion rate (mean of `converted`).
# MAGIC - Keep output concise with clear print labels.
# MAGIC - Do not overwrite `history_pdf` or `leads_pdf`.
# MAGIC - Remove any NotImplementedError stub.
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./genie/02_1_explore/code

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Prepare features for training
# MAGIC We treat this as a **classification problem**: given lead context + the action that was tried, predict `converted`.
# MAGIC At scoring time we will try every action for each lead and pick the highest probability.
# MAGIC
# MAGIC **Genie Code writes:** `genie/02_2_features/code`
# MAGIC
# MAGIC #### Genie Code prompt — copy everything inside the box
# MAGIC
# MAGIC ```text
# MAGIC Create or overwrite the Databricks notebook at genie/02_2_features/code in this lab folder.
# MAGIC Do not edit 02_Build_NBA_with_Genie_Code. Put ALL implementation in genie/02_2_features/code.
# MAGIC
# MAGIC Using `history_pdf` from the parent session, prepare training features for a Next Best Action model.
# MAGIC
# MAGIC Business framing:
# MAGIC - Target: `converted` (0/1)
# MAGIC - The model must see the candidate action as a feature named `action_taken`
# MAGIC - Possible actions: call_am, call_pm, sms, email
# MAGIC
# MAGIC Requirements:
# MAGIC - Use pandas only. No Spark UDFs, no GPU, no new clusters.
# MAGIC - Create a copy named `train_df`
# MAGIC - Fill null `days_since_last_contact` with -1
# MAGIC - Feature columns must be exactly:
# MAGIC   categorical: brand, product_interest, lead_source, state, age_band, action_taken
# MAGIC   numeric: lead_score, prior_contact_count, days_since_last_contact
# MAGIC - Create `X = train_df[feature_cols]` and `y = train_df["converted"].astype(int)`
# MAGIC - Print shape of X/y and value counts for y
# MAGIC - Define lists `cat_cols` and `num_cols` for later use in a sklearn ColumnTransformer
# MAGIC - Do not train a model in this file
# MAGIC - Remove any NotImplementedError stub
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./genie/02_2_features/code

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Train a fast sklearn model and log it to MLflow
# MAGIC
# MAGIC **Genie Code writes:** `genie/02_3_train/code`
# MAGIC
# MAGIC #### Genie Code prompt — copy everything inside the box
# MAGIC
# MAGIC ```text
# MAGIC Create or overwrite the Databricks notebook at genie/02_3_train/code in this lab folder.
# MAGIC Do not edit 02_Build_NBA_with_Genie_Code. Put ALL implementation in genie/02_3_train/code.
# MAGIC
# MAGIC Train a fast Next Best Action classifier with scikit-learn and log it to MLflow on Databricks.
# MAGIC
# MAGIC Inputs already available in the parent session after previous %run cells:
# MAGIC - X, y, cat_cols, num_cols
# MAGIC - MODEL_NAME, EXPERIMENT_PATH, RANDOM_SEED
# MAGIC - imports may already include mlflow, train_test_split, ColumnTransformer, OneHotEncoder, Pipeline, HistGradientBoostingClassifier, classification_report, roc_auc_score
# MAGIC
# MAGIC Requirements:
# MAGIC - Use a train/test split with test_size=0.2 and random_state=RANDOM_SEED, stratify=y
# MAGIC - Build a sklearn Pipeline:
# MAGIC   1) ColumnTransformer with sparse_threshold=0 that one-hot encodes cat_cols using OneHotEncoder(handle_unknown="ignore", sparse_output=False) and passes through num_cols
# MAGIC   2) HistGradientBoostingClassifier(max_depth=4, max_iter=80, learning_rate=0.1, random_state=RANDOM_SEED)
# MAGIC - Important: the matrix must be dense because HistGradientBoostingClassifier does not accept sparse input
# MAGIC - Fit on the training split only
# MAGIC - Print classification_report on the test split
# MAGIC - Print ROC AUC using predicted probabilities for class 1
# MAGIC - Set mlflow tracking URI to "databricks" and experiment to EXPERIMENT_PATH
# MAGIC - Log the fitted pipeline with mlflow.sklearn.log_model
# MAGIC - Register / log the model under name MODEL_NAME if straightforward with mlflow; otherwise log the run and print the run_id
# MAGIC - Keep everything CPU / local sklearn. No Spark ML, no GPU, no Model Serving endpoint creation
# MAGIC - Store the fitted pipeline in a variable called `model`
# MAGIC - Remove any NotImplementedError stub
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./genie/02_3_train/code

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Score today's leads and pick the Next Best Action
# MAGIC
# MAGIC **Genie Code writes:** `genie/02_4_score/code`
# MAGIC
# MAGIC #### Genie Code prompt — copy everything inside the box
# MAGIC
# MAGIC ```text
# MAGIC Create or overwrite the Databricks notebook at genie/02_4_score/code in this lab folder.
# MAGIC Do not edit 02_Build_NBA_with_Genie_Code. Put ALL implementation in genie/02_4_score/code.
# MAGIC
# MAGIC Using the fitted sklearn pipeline in `model` and pandas DataFrame `leads_pdf` from the parent session, create Next Best Action recommendations.
# MAGIC
# MAGIC Logic:
# MAGIC 1. For each lead, create 4 candidate rows - one per action in ACTIONS (call_am, call_pm, sms, email)
# MAGIC 2. Fill null days_since_last_contact with -1
# MAGIC 3. Predict conversion probability for every candidate with model.predict_proba(...)[:, 1]
# MAGIC 4. For each lead_id, keep the action with the highest probability as next_best_action
# MAGIC 5. Also keep the probability as expected_conversion_prob
# MAGIC 6. Write a pandas DataFrame `recs_pdf` with columns:
# MAGIC    lead_id, brand, product_interest, lead_source, state, age_band, lead_score,
# MAGIC    prior_contact_count, days_since_last_contact, next_best_action, expected_conversion_prob
# MAGIC 7. Write `recs_pdf` to the Unity Catalog table named in RECS_TABLE using spark.createDataFrame(recs_pdf).write.mode("overwrite").option("overwriteSchema","true").saveAsTable(RECS_TABLE)
# MAGIC 8. Print the top 15 recommendations sorted by expected_conversion_prob descending
# MAGIC 9. Print value counts for next_best_action
# MAGIC 10. Keep `cand_df` in the session (all candidate rows with conversion_prob) so a stretch cell can compare vs call_pm
# MAGIC
# MAGIC Constraints:
# MAGIC - pandas + existing `model` only for scoring
# MAGIC - no Model Serving, no applyInPandas requirement, no GPU
# MAGIC - keep it fast for ~400 leads x 4 actions
# MAGIC - Remove any NotImplementedError stub
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./genie/02_4_score/code

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Verify the output table (complete - run after 2.4 works)

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

display(
    spark.sql(
        f"""
        SELECT *
        FROM {RECS_TABLE}
        ORDER BY expected_conversion_prob DESC
        LIMIT 20
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stretch (optional, only if you finish early)
# MAGIC
# MAGIC **Genie Code writes:** `genie/02_5_stretch/code`
# MAGIC
# MAGIC #### Genie Code prompt — copy everything inside the box
# MAGIC
# MAGIC ```text
# MAGIC Create or overwrite the Databricks notebook at genie/02_5_stretch/code in this lab folder.
# MAGIC Do not edit 02_Build_NBA_with_Genie_Code. Put ALL implementation in genie/02_5_stretch/code.
# MAGIC
# MAGIC Add a short business summary for Greenstone call-centre leads.
# MAGIC
# MAGIC Using `recs_pdf` and `cand_df` from the parent session (or read RECS_TABLE into pandas):
# MAGIC - Estimate lift vs a naive baseline that always chooses call_pm
# MAGIC - To do that, for each lead also compute the predicted probability of call_pm from cand_df if available; if not, re-score quickly with the fitted `model`
# MAGIC - Print: number of leads, share recommended away from call_pm, mean expected_conversion_prob for NBA vs mean prob for call_pm
# MAGIC - Keep it under 30 lines, pandas only, no endpoints
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./genie/02_5_stretch/code

# COMMAND ----------

# MAGIC %md
# MAGIC ### Done
# MAGIC You built a Next Best Action flow with Genie Code: explore → features → train → batch score.
# MAGIC Each step lives as its own file under `genie/`, so you can open the diff and revert a single step without touching this notebook.
# MAGIC
# MAGIC **Roadmap tease for Greenstone:** this sits after lead scoring. Natural follow-ons are policy lapse/churn prediction and call-transcript intelligence over AQM audio.
# MAGIC
# MAGIC When finished, run **`99_Cleanup`**.