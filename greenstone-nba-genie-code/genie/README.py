# Databricks notebook source
# MAGIC %md
# MAGIC # `genie/` — files Genie Code should write
# MAGIC
# MAGIC Each subfolder is one lab step. `code` starts as a stub. Participants ask **Genie Code** to
# MAGIC fill `code` (not the parent notebook). Then they re-run the matching `%run` cell in `02_Build_NBA_with_Genie_Code`.
# MAGIC
# MAGIC | Folder | What it should define after Genie Code runs |
# MAGIC |---|---|
# MAGIC | `02_1_explore` | prints only (do not overwrite `history_pdf` / `leads_pdf`) |
# MAGIC | `02_2_features` | `train_df`, `cat_cols`, `num_cols`, `feature_cols`, `X`, `y` |
# MAGIC | `02_3_train` | `model` (fitted sklearn pipeline) |
# MAGIC | `02_4_score` | `recs_pdf`, `cand_df`; writes `RECS_TABLE` |
# MAGIC | `02_5_stretch` | optional lift vs always `call_pm` |
# MAGIC
# MAGIC **Revert:** restore the stub `code` notebook in that folder (or clone the lab folder again).
# MAGIC Do not edit `02_Build_NBA_with_Genie_Code` itself.
